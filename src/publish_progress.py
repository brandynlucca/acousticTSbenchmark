"""Publish compact timing snapshots to the data branch without a local checkout."""

import argparse
import base64
from datetime import datetime, timezone
import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import sys
import time
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener
import zipfile

from benchmark import ROOT, build_cases, read_json


BRANCH = "benchmark-data"
PLATFORMS = ("ubuntu-latest", "windows-latest", "macos-latest")
ROW_FIELDS = ("software", "boundary", "aspect_ratio", "incidence_deg", "frequency_hz",
              "k0a", "h0", "elapsed_seconds", "ts_db", "delta_ts_db", "status", "repetition")
MAX_JSON_BYTES = 128 * 1024 * 1024
MAX_ARCHIVE_BYTES = 256 * 1024 * 1024
MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024


class APIError(RuntimeError):
    def __init__(self, status, message):
        super().__init__(f"GitHub API HTTP {status}: {message}")
        self.status = status


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        # Artifact downloads use a signed storage URL. Never forward the token.
        return None


class GitHub:
    def __init__(self, repository, token, timeout_seconds=90):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise ValueError("Repository must be owner/repository")
        if not token:
            raise ValueError("GITHUB_TOKEN is required")
        self.repository = repository
        self.token = token
        self.deadline = time.monotonic() + timeout_seconds
        self.opener = build_opener(NoRedirect())

    def timeout(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Progress publication exceeded its network deadline")
        return min(20, remaining)

    def read(self, response, maximum):
        chunks, size = [], 0
        while True:
            self.timeout()
            chunk = response.read(min(1024 * 1024, maximum - size + 1))
            if not chunk:
                return b"".join(chunks)
            size += len(chunk)
            if size > maximum:
                raise ValueError("Progress response exceeds the supported size")
            chunks.append(chunk)

    def request(self, method, path, body=None):
        if not path.startswith("/") or path.startswith("//"):
            raise ValueError("GitHub API paths must be relative to the repository")
        data = None if body is None else json.dumps(body, allow_nan=False).encode("utf-8")
        request = Request(f"https://api.github.com/repos/{self.repository}{path}", data=data,
                          method=method, headers={"Accept": "application/vnd.github+json",
                          "Authorization": f"Bearer {self.token}",
                          "X-GitHub-Api-Version": "2022-11-28",
                          "User-Agent": "acousticTSbenchmark-progress",
                          "Content-Type": "application/json"})
        try:
            with self.opener.open(request, timeout=self.timeout()) as response:
                return json.loads(self.read(response, MAX_JSON_BYTES * 2))
        except HTTPError as error:
            # Do not include headers, tokens, signed URLs, or request bodies in errors.
            raise APIError(error.code, error.reason) from None

    def archive(self, artifact_id):
        request = Request(
            f"https://api.github.com/repos/{self.repository}/actions/artifacts/{int(artifact_id)}/zip",
            headers={"Authorization": f"Bearer {self.token}",
                     "Accept": "application/vnd.github+json",
                     "X-GitHub-Api-Version": "2022-11-28",
                     "User-Agent": "acousticTSbenchmark-progress"})
        try:
            response = self.opener.open(request, timeout=self.timeout())
        except HTTPError as error:
            if error.code not in (301, 302, 303, 307, 308):
                raise APIError(error.code, error.reason) from None
            location = error.headers.get("Location", "")
            target = urlparse(location)
            host = target.hostname or ""
            trusted = (host.endswith(".blob.core.windows.net") or
                       host.endswith(".actions.githubusercontent.com") or
                       host.endswith(".githubusercontent.com"))
            if target.scheme != "https" or target.username or not trusted:
                raise ValueError("GitHub returned an unsupported artifact storage host")
            # A fresh unauthenticated request prevents Authorization leaking to storage.
            try:
                response = self.opener.open(Request(location), timeout=self.timeout())
            except HTTPError as failure:
                raise APIError(failure.code, "Artifact download failed") from None
        with response:
            return self.read(response, MAX_ARCHIVE_BYTES)


def compact_payload(repository, platform, run_id, run_attempt, rows, status=None, state=None):
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("results.json must contain a list of records")
    if platform not in PLATFORMS or run_id < 1 or run_attempt < 1:
        raise ValueError("Invalid platform or workflow run identity")
    def finite_number(value):
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)

    aspects = sorted({row["aspect_ratio"] for row in rows if finite_number(row.get("aspect_ratio"))})
    angles = sorted({row["incidence_deg"] for row in rows if finite_number(row.get("incidence_deg"))})
    axes = {}
    for aspect in aspects:
        selected = [row for row in rows if row.get("aspect_ratio") == aspect]
        ranges = {}
        for field in ("frequency_hz", "h0", "k0a"):
            values = [row[field] for row in selected if finite_number(row.get(field))]
            if values:
                ranges[field] = [min(values), max(values)]
        ranges["frequencies_hz"] = sorted({row["frequency_hz"] for row in selected
                                            if finite_number(row.get("frequency_hz"))})
        axes[format(aspect, ".15g")] = ranges
    coverage = {}
    for row in rows:
        if not finite_number(row.get("aspect_ratio")) or not row.get("software") or not row.get("boundary"):
            continue
        key = (row["aspect_ratio"], row["software"], row["boundary"])
        group = coverage.setdefault(key, dict(aspect_ratio=key[0], software=key[1], boundary=key[2],
            planned=0, recorded=0, success=0, other_recorded=0, pending=0, elapsed_seconds_sum=0))
        group["planned"] += 1
        outcome = row.get("status", "pending")
        if outcome == "pending":
            group["pending"] += 1
        else:
            group["recorded"] += 1
            group["success" if outcome == "ok" else "other_recorded"] += 1
            elapsed = row.get("elapsed_seconds")
            if outcome == "ok" and finite_number(elapsed) and elapsed >= 0:
                group["elapsed_seconds_sum"] += elapsed
    compact = []
    for row in rows:
        if row.get("status", "pending") == "pending":
            continue
        item = {}
        for key in ROW_FIELDS:
            value = row.get(key)
            if not isinstance(value, (str, int, float, bool, type(None))):
                raise ValueError(f"Unexpected structured value in public field {key}")
            item[key] = None if isinstance(value, float) and not math.isfinite(value) else value
        compact.append(item)
    original = status or {}
    if not isinstance(original, dict):
        raise ValueError("status.json must contain an object")
    safe_status = {key: original[key] for key in ("state", "updated_at", "planned", "recorded", "outcomes")
                   if key in original}
    safe_status.setdefault("planned", len(rows))
    safe_status["recorded"] = len(compact)
    counts = {}
    for row in compact:
        outcome = row["status"]
        counts[outcome] = counts.get(outcome, 0) + 1
    safe_status["outcomes"] = dict(counts, pending=len(rows) - len(compact))
    if state is None:
        state = "completed" if original.get("state") == "complete" else "running"
    if state not in ("setup", "running", "completed"):
        raise ValueError("State must be setup, running, or completed")
    run_url = f"https://github.com/{repository}/actions/runs/{run_id}"
    result = dict(schema_version=1, run_id=run_id, run_attempt=run_attempt, run_url=run_url,
                  artifacts_url=run_url + "#artifacts", platform=platform,
                  published_at=datetime.now(timezone.utc).isoformat(), state=state,
                  status=safe_status, status_counts=counts, axes=axes,
                  aspect_ratios=aspects, incidences_deg=angles,
                  coverage=list(coverage.values()), rows=compact)
    json.dumps(result, allow_nan=False)
    return result


def pending_setup_cases(config_path, profile, software=None, repetitions=None):
    """Describe an explicitly selected campaign before any worker is launched."""
    configuration = read_json(config_path)
    if not isinstance(configuration, dict) or not isinstance(configuration.get("profiles"), dict):
        raise ValueError("Invalid campaign configuration: profiles must be an object")
    if not profile or profile not in configuration["profiles"]:
        raise ValueError("Setup publication requires a known --profile or PROFILE")
    try:
        cases = build_cases(configuration, profile,
                            software.split(",") if software else None, repetitions)
    except (KeyError, TypeError, AttributeError) as error:
        raise ValueError(f"Invalid campaign configuration: {error}") from error
    if not cases:
        raise ValueError("The selected campaign contains no cases")
    return [dict(case, status="pending") for case in cases]


def should_publish(current, proposed):
    if current is None:
        return True
    old = (int(current["run_id"]), int(current.get("run_attempt", 1)))
    new = (proposed["run_id"], proposed["run_attempt"])
    if new != old:
        return new > old
    if proposed["status"]["recorded"] < current.get("status", {}).get("recorded", 0):
        return False
    rank = {"setup": 0, "running": 1, "completed": 2}
    if rank[proposed["state"]] < rank.get(current.get("state"), 0):
        return False
    previous_time = current.get("status", {}).get("updated_at", "")
    next_time = proposed["status"].get("updated_at", "")
    return not previous_time or not next_time or next_time >= previous_time


def select_artifact(artifacts, platform):
    checkpoint = re.compile(rf"checkpoint-{re.escape(platform)}-\d+-\d+\Z")
    candidates = [artifact for artifact in artifacts if not artifact.get("expired", False)
                  and (artifact["name"] == f"results-{platform}" or
                       checkpoint.fullmatch(artifact["name"]))]
    return max(candidates, key=lambda artifact: int(artifact["id"]), default=None)


def artifact_payload(api, platform, run_id, state=None):
    run = api.request("GET", f"/actions/runs/{run_id}")
    artifacts = []
    for page in range(1, 101):
        part = api.request("GET", f"/actions/runs/{run_id}/artifacts?per_page=100&page={page}")["artifacts"]
        artifacts.extend(part)
        if len(part) < 100:
            break
    selected = select_artifact(artifacts, platform)
    if selected is None:
        payload = compact_payload(api.repository, platform, run_id, int(run.get("run_attempt", 1)),
                                  [], {"state": run.get("status", "queued")},
                                  state or ("completed" if run.get("status") == "completed" else "setup"))
        payload.update(run_state=run.get("status"), run_conclusion=run.get("conclusion"))
        return payload
    attempt_match = re.fullmatch(rf"checkpoint-{re.escape(platform)}-(\d+)-\d+", selected["name"])
    attempt = int(attempt_match.group(1)) if attempt_match else int(run.get("run_attempt", 1))
    if (not attempt_match and attempt > 1 and
            selected.get("created_at", "") < run.get("run_started_at", "")):
        raise ValueError("The final artifact predates this run attempt. Import its checkpoint instead")
    values = {}
    with zipfile.ZipFile(io.BytesIO(api.archive(selected["id"]))) as archive:
        for entry in archive.infolist():
            name = PurePosixPath(entry.filename).name
            if name not in ("results.json", "status.json"):
                continue
            if name in values or entry.file_size > MAX_JSON_BYTES:
                raise ValueError("Ambiguous or oversized progress JSON in artifact")
            values[name] = json.loads(archive.read(entry))
    if "results.json" not in values:
        raise ValueError("Selected artifact contains no results.json")
    inferred = "completed" if run.get("status") == "completed" else None
    payload = compact_payload(api.repository, platform, run_id, attempt, values["results.json"],
                              values.get("status.json"), state or inferred)
    payload["artifact_id"] = selected["id"]
    payload["artifact_name"] = selected["name"]
    payload["artifact_url"] = payload["run_url"] + f"/artifacts/{selected['id']}"
    payload.update(run_state=run.get("status"), run_conclusion=run.get("conclusion"))
    return payload


def existing_snapshot(api, platform):
    path = f"/contents/{platform}.json?" + urlencode({"ref": BRANCH})
    try:
        content = api.request("GET", path)
    except APIError as error:
        if error.status == 404:
            return None, None
        raise
    # The Contents API omits inline content for files larger than 1 MB.
    if int(content.get("size", 0)) > MAX_SNAPSHOT_BYTES:
        raise ValueError("Existing progress snapshot exceeds the supported size")
    value = content
    if content.get("encoding") == "none" or not content.get("content"):
        value = api.request("GET", f"/git/blobs/{content['sha']}")
    if value.get("encoding") != "base64":
        raise ValueError("GitHub returned an unsupported content encoding")
    encoded = "".join(value["content"].split())
    if (int(value.get("size", 0)) > MAX_SNAPSHOT_BYTES or
            len(encoded) > 4 * ((MAX_SNAPSHOT_BYTES + 2) // 3)):
        raise ValueError("Existing progress snapshot exceeds the supported size")
    decoded = base64.b64decode(encoded, validate=True)
    if len(decoded) > MAX_SNAPSHOT_BYTES:
        raise ValueError("Existing progress snapshot exceeds the supported size")
    return json.loads(decoded), content["sha"]


def publish(api, payload):
    platform = payload["platform"]
    serialized = json.dumps(payload, allow_nan=False, separators=(",", ":")).encode()
    if len(serialized) > MAX_SNAPSHOT_BYTES:
        raise ValueError("Progress snapshot exceeds the supported size")
    encoded = base64.b64encode(serialized).decode()
    message = f"Update {platform} progress for run {payload['run_id']} attempt {payload['run_attempt']}"
    for _ in range(4):
        try:
            api.request("GET", f"/git/ref/heads/{BRANCH}")
        except APIError as error:
            if error.status != 404:
                raise
            blob = api.request("POST", "/git/blobs", dict(content=encoded, encoding="base64"))
            tree = api.request("POST", "/git/trees", {"tree": [dict(path=f"{platform}.json",
                               mode="100644", type="blob", sha=blob["sha"])]})
            commit = api.request("POST", "/git/commits", dict(message=message, tree=tree["sha"], parents=[]))
            try:
                api.request("POST", "/git/refs", dict(ref=f"refs/heads/{BRANCH}", sha=commit["sha"]))
                return True
            except APIError as conflict:
                if conflict.status not in (409, 422):
                    raise
                continue
        current, sha = existing_snapshot(api, platform)
        if not should_publish(current, payload):
            return False
        body = dict(message=message, content=encoded, branch=BRANCH)
        if sha is not None:
            body["sha"] = sha
        try:
            api.request("PUT", f"/contents/{platform}.json", body)
            return True
        except APIError as error:
            if error.status not in (409, 422):
                raise
    raise RuntimeError("Progress changed concurrently too often. Retry publication")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"), required=not os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--platform", choices=PLATFORMS, required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--directory", type=Path)
    source.add_argument("--from-artifact", type=int, metavar="RUN_ID")
    parser.add_argument("--run-id", type=int, default=os.environ.get("GITHUB_RUN_ID"))
    parser.add_argument("--run-attempt", type=int, default=os.environ.get("GITHUB_RUN_ATTEMPT", "1"))
    parser.add_argument("--state", choices=("setup", "running", "completed"))
    parser.add_argument("--config", type=Path, default=ROOT / "benchmark.json",
                        help="Campaign configuration for setup publication before results exist")
    parser.add_argument("--profile", default=os.environ.get("PROFILE"),
                        help="Setup campaign profile, or PROFILE from the workflow")
    parser.add_argument("--software", default=os.environ.get("SOFTWARE"),
                        help="Setup software selection, or SOFTWARE from the workflow")
    parser.add_argument("--repetitions", type=int, default=os.environ.get("REPETITIONS"),
                        help="Setup repetitions, or REPETITIONS from the workflow")
    args = parser.parse_args()
    api = GitHub(args.repository, os.environ.get("GITHUB_TOKEN"))
    if args.from_artifact is not None:
        if args.from_artifact < 1:
            parser.error("--from-artifact requires a positive run ID")
        payload = artifact_payload(api, args.platform, args.from_artifact, args.state)
    else:
        if args.run_id is None:
            parser.error("--run-id or GITHUB_RUN_ID is required for local snapshots")
        results_path, status_path = args.directory / "results.json", args.directory / "status.json"
        if not results_path.exists() and args.state != "setup":
            raise ValueError("No results.json is available. Only setup publication can omit results")
        if results_path.exists():
            rows = json.loads(results_path.read_text(encoding="utf-8"))
            status = json.loads(status_path.read_text(encoding="utf-8")) if status_path.exists() else {}
        else:
            rows = pending_setup_cases(args.config, args.profile, args.software, args.repetitions)
            status = dict(state="setup", planned=len(rows), recorded=0,
                          updated_at=datetime.now(timezone.utc).isoformat())
        payload = compact_payload(args.repository, args.platform, args.run_id, args.run_attempt,
                                  rows, status, args.state)
    changed = publish(api, payload)
    print(f"{'Published' if changed else 'Kept newer snapshot for'} {args.platform} progress")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, OSError, zipfile.BadZipFile) as error:
        print(f"Progress publication failed: {error}", file=sys.stderr)
        raise SystemExit(1)
