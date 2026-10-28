"""Publish compact timing snapshots to the data branch without a local checkout."""

import argparse
import base64
from collections import OrderedDict
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener
import zipfile

from benchmark import CAPABILITIES, ROOT, STATUSES, build_cases, load_references, read_json, summarize


BRANCH = "benchmark-data"
PLATFORMS = ("ubuntu-latest", "windows-latest", "macos-latest")
SEMIMAJOR_OPTIONS = (.001, .01, .07, .1, .5)
ROW_FIELDS = ("software", "boundary", "semimajor_m", "aspect_ratio", "incidence_deg", "frequency_hz",
              "k0a", "h0", "elapsed_seconds", "ts_db", "delta_ts_db", "status", "repetition",
              "physical_id", "reference_id", "reference_ts_db", "reference_uncertainty_db")
ROW_FIELDS += ("memory_delta_peak_bytes", "memory_baseline_rss_bytes", "memory_peak_rss_bytes",
               "memory_sample_interval_seconds", "memory_samples", "memory_status", "memory_method")
ROW_FIELDS += ("source_sha", "measurement_role", "retained_from_run_id")
MAX_JSON_BYTES = 128 * 1024 * 1024
# Older continuations embedded the full reference plan in every provenance
# manifest. Keep those saved measurements readable while new baselines omit
# that duplicate plan. Other progress metadata retains its smaller bound.
MAX_RETAINED_BYTES = 512 * 1024 * 1024
MAX_REFERENCE_BYTES = 512 * 1024 * 1024
MAX_ARCHIVE_BYTES = 256 * 1024 * 1024
MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024
SNAPSHOT_COMPRESSION_THRESHOLD = 64 * 1024
SNAPSHOT_GUARD_FIELDS = ("run_id", "run_attempt", "platform", "semimajor_m", "published_at", "state", "status")
_SNAPSHOT_GUARDS = OrderedDict()


class APIError(RuntimeError):
    def __init__(self, status, message, *, method=None, path=None):
        super().__init__(f"GitHub API HTTP {status}: {message}")
        self.status = status
        self.method = method
        self.path = path


def safe_error_summary(error, api=None):
    """Describe network failures without including remote URLs or credentials."""
    description = type(error).__name__
    if isinstance(error, APIError):
        description += f" HTTP {error.status}"
    if isinstance(error, URLError) and isinstance(getattr(error.reason, "errno", None), int):
        description += f" errno {error.reason.errno}"
    request = getattr(api, "last_request", None)
    method, path = getattr(error, "method", None), getattr(error, "path", None)
    if not method and isinstance(request, tuple) and len(request) == 2:
        method, path = request
    if isinstance(path, str):
        path = path.split("?", 1)[0]
    if method in ("GET", "POST", "PUT", "PATCH", "DELETE") and isinstance(path, str) and re.fullmatch(r"/[A-Za-z0-9_./-]+", path):
        description += f" during {method} {path}"
    return description


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
        self.last_request = (method, path)
        data = None if body is None else json.dumps(body, allow_nan=False).encode("utf-8")
        request = Request(f"https://api.github.com/repos/{self.repository}{path}", data=data,
                          method=method, headers={"Accept": "application/vnd.github+json",
                          "Authorization": f"Bearer {self.token}",
                          "X-GitHub-Api-Version": "2022-11-28",
                          "User-Agent": "acousticTSbenchmark-progress",
                          "Content-Type": "application/json"})
        try:
            with self.opener.open(request, timeout=self.timeout()) as response:
                content = self.read(response, MAX_JSON_BYTES * 2)
                return json.loads(content) if content else None
        except HTTPError as error:
            # Do not include headers, tokens, signed URLs, or request bodies in errors.
            raise APIError(error.code, error.reason, method=method, path=path) from None
        except OSError as error:
            error.method, error.path = method, path
            raise

    def archive(self, artifact_id):
        return self._download_archive(f"/actions/artifacts/{int(artifact_id)}/zip")

    def repository_archive(self, commit_sha):
        if not re.fullmatch(r"[a-f0-9]{40}", commit_sha):
            raise ValueError("Repository archives require an exact commit SHA")
        return self._download_archive(f"/zipball/{commit_sha}")

    def _download_archive(self, path):
        request = Request(
            f"https://api.github.com/repos/{self.repository}{path}",
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
            trusted = (host == "codeload.github.com" or host.endswith(".blob.core.windows.net") or
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


def json_size_limit(name):
    if name == "retained-results.json":
        return MAX_RETAINED_BYTES
    if name == "references.json":
        return MAX_REFERENCE_BYTES
    return MAX_JSON_BYTES


def read_retained(path):
    path = Path(path)
    if path.stat().st_size > MAX_RETAINED_BYTES:
        raise ValueError("Retained measurements exceed the supported size")
    return read_json(path)


def write_retained(path, value):
    """Reject oversized baselines before installing them or starting workers."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    size = 0
    try:
        with temporary.open("wb") as stream:
            encoder = json.JSONEncoder(allow_nan=False, separators=(",", ":"))
            for chunk in encoder.iterencode(value):
                encoded = chunk.encode("utf-8")
                size += len(encoded)
                if size > MAX_RETAINED_BYTES:
                    raise ValueError("Retained measurements exceed the supported size")
                stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def retained_comparison_rows(sidecar, active_rows, references=None):
    """Validate saved measurements and derive comparisons without changing their provenance."""
    if (not isinstance(sidecar, dict) or sidecar.get("schema_version") != 1 or
            not isinstance(sidecar.get("rows"), list) or
            not isinstance(sidecar.get("provenance"), list) or
            any(not isinstance(item, dict) for item in sidecar["provenance"])):
        raise ValueError("Invalid retained measurement metadata")
    programs = {row.get("software") for row in active_rows}
    identifiers = {row.get("case_id") for row in active_rows}
    retained = []
    for row in sidecar["rows"]:
        if (not isinstance(row, dict) or row.get("measurement_role") != "retained" or
                row.get("status") not in STATUSES or
                row.get("software") not in CAPABILITIES or row["software"] == "acousticscattering" or
                row["software"] in programs or
                not isinstance(row.get("case_id"), str) or
                not re.fullmatch(r"[a-f0-9]{64}", row["case_id"]) or row["case_id"] in identifiers or
                not isinstance(row.get("source_sha"), str) or
                not re.fullmatch(r"[a-f0-9]{40}", row["source_sha"]) or
                type(row.get("retained_from_run_id")) is not int or row["retained_from_run_id"] < 1):
            raise ValueError("Invalid, overlapping, or incomplete retained measurement")
        identifiers.add(row["case_id"])
        retained.append(dict(row))
    json.dumps(retained, allow_nan=False)
    if references is not None:
        updated = []
        for row in retained:
            if row.get("physical_id") in references or not curves_from_rows([row]):
                case = {key: value for key, value in row.items()
                        if not key.startswith("reference_") and key != "delta_ts_db"}
                row = summarize([case], {}, references)[0]
            updated.append(row)
        retained = updated
    return retained


def comparison_rows(directory, rows, references=None):
    """Append retained comparison measurements to an active-only case table."""
    if (not isinstance(rows, list) or any(not isinstance(row, dict) or
            row.get("measurement_role") == "retained" for row in rows)):
        raise ValueError("Comparison input must contain only active measurement rows")
    directory = Path(directory)
    path = directory / "retained-results.json"
    if not path.is_file():
        return rows
    if references is None and (directory / "references.json").is_file():
        references = load_references(directory / "references.json")
    return rows + retained_comparison_rows(read_retained(path), rows, references)


def curves_from_rows(rows):
    """Keep one qualified reference per physical prediction, including pending cases."""
    curves = {}
    for row in rows:
        ts = row.get("reference_ts_db")
        if (not row.get("reference_id") or not row.get("physical_id") or
                not isinstance(ts, (int, float)) or isinstance(ts, bool) or
                not math.isfinite(ts) or not row.get("reference_qualification")):
            continue
        fields = ("physical_id", "reference_id", "boundary", "semimajor_m", "aspect_ratio",
                  "incidence_deg", "frequency_hz", "k0a", "h0")
        item = {key: row.get(key) for key in fields}
        item.update(ts_db=ts, uncertainty_db=row.get("reference_uncertainty_db"),
                    source_sha=row.get("reference_source_sha"),
                    method="fem_bem" if row.get("boundary") in ("gas", "liquid") else "bem")
        previous = curves.setdefault(item["physical_id"], item)
        if previous != item:
            raise ValueError("Conflicting TS references for one physical prediction")
    return list(curves.values())


def compact_payload(repository, platform, run_id, run_attempt, rows, status=None, state=None):
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("results.json must contain a list of records")
    if platform not in PLATFORMS or run_id < 1 or run_attempt < 1:
        raise ValueError("Invalid platform or workflow run identity")
    active_rows = [row for row in rows if row.get("measurement_role") != "retained"]
    retained_rows = [row for row in rows if row.get("measurement_role") == "retained"]
    if retained_rows:
        retained_comparison_rows(dict(schema_version=1, rows=retained_rows, provenance=[]), active_rows)
    def finite_number(value):
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)

    sizes = set()
    for row in rows:
        size = row.get("semimajor_m")
        if size is None:
            continue
        if not finite_number(size) or size <= 0:
            raise ValueError("semimajor_m must be a finite positive number")
        sizes.add(size)
    if len(sizes) > 1:
        raise ValueError("A progress snapshot must contain only one semimajor axis")
    semimajor = next(iter(sizes), None)
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
        software_frequencies = {
            software: sorted({row["frequency_hz"] for row in selected
                              if row.get("software") == software and finite_number(row.get("frequency_hz"))})
            for software in sorted({row["software"] for row in selected if row.get("software")})}
        if software_frequencies:
            ranges["software_frequencies_hz"] = software_frequencies
        axes[format(aspect, ".15g")] = ranges
    coverage, incidence_coverage = {}, {}
    for row in rows:
        if not finite_number(row.get("aspect_ratio")) or not row.get("software") or not row.get("boundary"):
            continue
        key = (row["aspect_ratio"], row["software"], row["boundary"])
        empty = dict(aspect_ratio=key[0], software=key[1], boundary=key[2],
                     planned=0, recorded=0, success=0, other_recorded=0, pending=0, elapsed_seconds_sum=0)
        groups = [coverage.setdefault(key, dict(empty))]
        if finite_number(row.get("incidence_deg")):
            angle_key = (*key, row["incidence_deg"])
            groups.append(incidence_coverage.setdefault(angle_key, dict(empty, incidence_deg=angle_key[-1])))
        for group in groups:
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
    safe_status.setdefault("planned", len(active_rows))
    active_compact = [row for row in compact if row.get("measurement_role") != "retained"]
    safe_status["recorded"] = len(active_compact)
    counts = {}
    for row in active_compact:
        outcome = row["status"]
        counts[outcome] = counts.get(outcome, 0) + 1
    safe_status["outcomes"] = dict(counts, pending=len(active_rows) - len(active_compact))
    if state is None:
        state = "completed" if original.get("state") == "complete" else "running"
    if state not in ("setup", "running", "completed"):
        raise ValueError("State must be setup, running, or completed")
    run_url = f"https://github.com/{repository}/actions/runs/{run_id}"
    result = dict(schema_version=1, run_id=run_id, run_attempt=run_attempt, run_url=run_url,
                  artifacts_url=run_url + "#artifacts", platform=platform,
                  published_at=datetime.now(timezone.utc).isoformat(), state=state,
                  status=safe_status, status_counts=counts, axes=axes,
                  semimajor_m=semimajor,
                  aspect_ratios=aspects, incidences_deg=angles,
                  coverage=list(coverage.values()), incidence_coverage=list(incidence_coverage.values()), rows=compact,
                  reference_curves=curves_from_rows(rows))
    if retained_rows:
        retained_sources = sorted({(row["software"], row["source_sha"], row["retained_from_run_id"])
                                   for row in retained_rows})
        result.update(active_software=sorted({row["software"] for row in active_rows if row.get("software")}),
                      retained_count=len(retained_rows), retained_sources=[
                          dict(software=name, source_sha=sha, run_id=run) for name, sha, run in retained_sources])
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


def setup_measurement_rows(directory, rows, platform):
    """Keep validated selected-software measurements visible during continuation setup."""
    directory = Path(directory)
    path = directory / "retained-results.json"
    if not path.is_file():
        return rows
    sidecar = read_retained(path)
    if "reused_records" not in sidecar:
        return rows
    from campaign import MAX_RECORD_BYTES
    from refresh import validate_reused_records

    cases = [{key: value for key, value in row.items() if key != "status"} for row in rows]
    records = validate_reused_records(sidecar, cases, read_json(ROOT / "sources.lock.json"),
        platform={"ubuntu-latest": "Linux", "windows-latest": "Windows", "macos-latest": "Darwin"}[platform])
    for identity, record in records.items():
        saved = directory / "records" / f"{identity}.json"
        if (not saved.is_file() or saved.stat().st_size > MAX_RECORD_BYTES or read_json(saved) != record):
            raise ValueError("Setup measurements do not match their preserved records")
    reference_path = directory / "references.json"
    references = load_references(reference_path) if reference_path.is_file() else {}
    return summarize(cases, records, references)


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
                  and (artifact["name"] in (f"results-{platform}", f"compared-results-{platform}", f"retained-{platform}") or
                       checkpoint.fullmatch(artifact["name"]))]
    # Artifact IDs are unique identifiers, not a chronological sequence.
    return max(candidates, key=lambda artifact: (artifact.get("created_at", ""),
               int(artifact["id"])), default=None)


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
    values, baseline = {}, None
    content = api.archive(selected["id"])
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        if not any(PurePosixPath(entry.filename).name == "results.json" for entry in archive.infolist()):
            from refresh import baseline_settings

            baseline = baseline_settings(content, api.repository, run_id, platform)
            if baseline is None:
                raise ValueError("Selected artifact contains no results.json or validated setup baseline")
        for entry in archive.infolist():
            name = PurePosixPath(entry.filename).name
            if name not in ("results.json", "status.json", "retained-results.json", "references.json"):
                continue
            if name in values or entry.file_size > json_size_limit(name):
                raise ValueError("Ambiguous or oversized progress JSON in artifact")
            values[name] = json.loads(archive.read(entry))
    inferred = "completed" if run.get("status") == "completed" else None
    references = None
    if "references.json" in values:
        from benchmark import references_from_value
        references = references_from_value(values["references.json"])
    if baseline is not None:
        cases = build_cases(baseline["configuration"], baseline["profile"],
                            baseline["software"].split(","), baseline["repetitions"])
        rows = summarize(cases, values["retained-results.json"].get("reused_records", {}), references or {})
        values["status.json"] = dict(state="setup_failed" if inferred else "setup", planned=len(cases))
    else:
        rows = values["results.json"]
    if "retained-results.json" in values:
        rows = rows + retained_comparison_rows(values["retained-results.json"], rows, references)
    payload = compact_payload(api.repository, platform, run_id, attempt, rows,
                              values.get("status.json"), state or inferred)
    payload["artifact_id"] = selected["id"]
    payload["artifact_name"] = selected["name"]
    payload["artifact_url"] = payload["run_url"] + f"/artifacts/{selected['id']}"
    payload.update(run_state=run.get("status"), run_conclusion=run.get("conclusion"))
    return payload


def snapshot_filename(platform, semimajor=None):
    if platform not in PLATFORMS:
        raise ValueError("Invalid progress snapshot platform")
    if semimajor is None:
        return f"{platform}.json"
    if isinstance(semimajor, bool) or semimajor not in SEMIMAJOR_OPTIONS:
        raise ValueError("Unsupported semimajor axis for a saved progress snapshot")
    return f"{platform}-a-{format(semimajor, '.15g')}.json"


def encode_snapshot(payload):
    """Keep small snapshots readable and compress large snapshots losslessly."""
    serialized = json.dumps(payload, allow_nan=False, separators=(",", ":")).encode("utf-8")
    if len(serialized) > MAX_SNAPSHOT_BYTES:
        raise ValueError("Progress snapshot exceeds the supported size")
    if len(serialized) <= SNAPSHOT_COMPRESSION_THRESHOLD:
        return dict(payload)
    envelope = {key: payload.get(key) for key in SNAPSHOT_GUARD_FIELDS}
    envelope.update(schema_version=2, encoding="gzip-base64", decoded_bytes=len(serialized),
        data=base64.b64encode(gzip.compress(serialized, compresslevel=6, mtime=0)).decode("ascii"))
    if len(json.dumps(envelope, allow_nan=False, separators=(",", ":")).encode("utf-8")) > MAX_SNAPSHOT_BYTES:
        raise ValueError("Encoded progress snapshot exceeds the supported size")
    return envelope


def decode_snapshot(value):
    """Decode a bounded compressed envelope and validate its ordering metadata."""
    if not isinstance(value, dict):
        raise ValueError("Progress snapshot must contain an object")
    if value.get("schema_version") != 2:
        return value
    expected = value.get("decoded_bytes")
    encoded = value.get("data")
    if (value.get("encoding") != "gzip-base64" or type(expected) is not int
            or not 0 < expected <= MAX_SNAPSHOT_BYTES or not isinstance(encoded, str)
            or len(encoded) > 4 * ((MAX_SNAPSHOT_BYTES + 2) // 3)):
        raise ValueError("Invalid compressed progress snapshot")
    compressed = base64.b64decode(encoded, validate=True)
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:
        decoded = stream.read(expected + 1)
    if len(decoded) != expected:
        raise ValueError("Decoded progress snapshot has an invalid size")
    result = json.loads(decoded)
    if (not isinstance(result, dict) or result.get("schema_version") != 1
            or any(result.get(key) != value.get(key) for key in SNAPSHOT_GUARD_FIELDS)):
        raise ValueError("Decoded progress snapshot disagrees with its publication metadata")
    return result


def _blob_snapshot(value):
    if value.get("encoding") != "base64":
        raise ValueError("GitHub returned an unsupported content encoding")
    encoded = "".join(value["content"].split())
    if (int(value.get("size", 0)) > MAX_SNAPSHOT_BYTES or
            len(encoded) > 4 * ((MAX_SNAPSHOT_BYTES + 2) // 3)):
        raise ValueError("Existing progress snapshot exceeds the supported size")
    decoded = base64.b64decode(encoded, validate=True)
    if len(decoded) > MAX_SNAPSHOT_BYTES:
        raise ValueError("Existing progress snapshot exceeds the supported size")
    result = json.loads(decoded)
    if not isinstance(result, dict):
        raise ValueError("Progress snapshot must contain an object")
    return result


def existing_snapshot(api, platform, filename=None):
    allowed = {snapshot_filename(platform), *(snapshot_filename(platform, size) for size in SEMIMAJOR_OPTIONS)}
    filename = snapshot_filename(platform) if filename is None else filename
    if filename not in allowed:
        raise ValueError("Invalid progress snapshot filename")
    path = f"/contents/{filename}?" + urlencode({"ref": BRANCH})
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
    return decode_snapshot(_blob_snapshot(value)), content["sha"]


def _cache_guard(api, sha, value):
    key = (api.repository, sha)
    guard = {field: value.get(field) for field in SNAPSHOT_GUARD_FIELDS}
    if not isinstance(guard.get("status"), dict):
        raise ValueError("Progress publication metadata lacks its status")
    guard["status"] = {field: guard["status"].get(field) for field in ("recorded", "updated_at")}
    _SNAPSHOT_GUARDS[key] = guard
    _SNAPSHOT_GUARDS.move_to_end(key)
    while len(_SNAPSHOT_GUARDS) > 64:
        _SNAPSHOT_GUARDS.popitem(last=False)
    return _SNAPSHOT_GUARDS[key]


def _snapshot_guard(api, entry):
    if entry is None:
        return None
    if entry.get("type") != "blob" or entry.get("size", 0) > MAX_SNAPSHOT_BYTES:
        raise ValueError("Invalid or oversized progress snapshot tree entry")
    key = (api.repository, entry["sha"])
    if key in _SNAPSHOT_GUARDS:
        _SNAPSHOT_GUARDS.move_to_end(key)
        return _SNAPSHOT_GUARDS[key]
    value = _blob_snapshot(api.request("GET", f"/git/blobs/{entry['sha']}"))
    if value.get("schema_version") == 2 and value.get("encoding") != "gzip-base64":
        raise ValueError("Unsupported compressed progress snapshot")
    return _cache_guard(api, entry["sha"], value)


def _progress_tree(api):
    try:
        head = api.request("GET", f"/git/ref/heads/{BRANCH}")["object"]["sha"]
    except APIError as error:
        if error.status == 404:
            return None, None, {}
        raise
    commit = api.request("GET", f"/git/commits/{head}")
    tree_sha = commit["tree"]["sha"]
    tree = api.request("GET", f"/git/trees/{tree_sha}")
    if tree.get("truncated"):
        raise ValueError("Progress branch tree listing is incomplete")
    return head, tree_sha, {entry["path"]: entry for entry in tree["tree"]}


def publish(api, payload):
    """Atomically update both progress aliases with one compressed Git blob."""
    platform = payload["platform"]
    names = [snapshot_filename(platform)]
    semimajor = payload.get("semimajor_m")
    if not isinstance(semimajor, bool) and semimajor in SEMIMAJOR_OPTIONS:
        names.insert(0, snapshot_filename(platform, semimajor))
    value = encode_snapshot(payload)
    serialized = json.dumps(value, allow_nan=False, separators=(",", ":")).encode("utf-8")
    encoded = base64.b64encode(serialized).decode()
    proposed_sha = hashlib.sha1(f"blob {len(serialized)}\0".encode() + serialized).hexdigest()
    _cache_guard(api, proposed_sha, payload)
    blob = None
    message = f"Update {platform} progress for run {payload['run_id']} attempt {payload['run_attempt']}"
    for attempt in range(4):
        head, tree_sha, entries = _progress_tree(api)
        selected = [name for name in names if entries.get(name, {}).get("sha") != proposed_sha
                    and should_publish(_snapshot_guard(api, entries.get(name)), payload)]
        if not selected:
            return False
        if blob is None:
            blob = api.request("POST", "/git/blobs", dict(content=encoded, encoding="base64"))
            if blob["sha"] != proposed_sha:
                raise ValueError("Published progress blob does not match its content")
        body = dict(tree=[dict(path=name, mode="100644", type="blob", sha=blob["sha"]) for name in selected])
        if tree_sha:
            body["base_tree"] = tree_sha
        tree = api.request("POST", "/git/trees", body)
        commit = api.request("POST", "/git/commits", dict(message=message, tree=tree["sha"],
                             parents=[head] if head else []))
        try:
            if head:
                api.request("PATCH", f"/git/refs/heads/{BRANCH}", dict(sha=commit["sha"], force=False))
            else:
                api.request("POST", "/git/refs", dict(ref=f"refs/heads/{BRANCH}", sha=commit["sha"]))
            return True
        except APIError as error:
            if error.status not in (409, 422):
                raise
            if attempt < 3:
                time.sleep(.1 * (attempt + 1))
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
            rows = setup_measurement_rows(args.directory, rows, args.platform)
            status = dict(state="setup", planned=len(rows),
                          recorded=sum(row["status"] != "pending" for row in rows),
                          updated_at=datetime.now(timezone.utc).isoformat())
        payload = compact_payload(args.repository, args.platform, args.run_id, args.run_attempt,
                                  comparison_rows(args.directory, rows), status, args.state)
    api = GitHub(args.repository, os.environ.get("GITHUB_TOKEN"), timeout_seconds=45)
    changed = publish(api, payload)
    print(f"{'Published' if changed else 'Kept newer snapshot for'} {args.platform} progress")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, OSError, zipfile.BadZipFile) as error:
        print(f"Progress publication failed: {safe_error_summary(error)}", file=sys.stderr)
        raise SystemExit(1)
