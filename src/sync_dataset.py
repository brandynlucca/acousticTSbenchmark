"""Collect immutable Actions artifacts into a durable, downloadable SQLite dataset."""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import sys
import time
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse
from urllib.request import Request

from benchmark import utc_now
from dataset import connect_database, database_summary, export_csv, export_comparisons_csv, import_artifact
from publish_progress import APIError, BRANCH, GitHub

CATALOG = "dataset.json"
RELEASE_TAG = "benchmark-dataset"
MAX_DOWNLOAD = 2 * 1024 ** 3 - 1
MAX_DATABASE = 8 * 1024 ** 3
MAX_CATALOG = 4 * 1024 ** 2
ASSET_NAME = re.compile(r"dataset-\d+-[a-f0-9]+-(benchmarks\.sqlite|measurements\.csv|comparisons\.csv)\.gz\Z")
TIMING_NAME = re.compile(r"(results|retained|compared-results)-(ubuntu-latest|windows-latest|macos-latest)\Z")
CHECKPOINT_NAME = re.compile(r"checkpoint-(ubuntu-latest|windows-latest|macos-latest)-(\d+)-(\d+)\Z")
REFERENCE_NAME = re.compile(r"references-\d+-\d+\Z")


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class DatasetAPI(GitHub):
    def timeout(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Dataset collection exceeded its network deadline")
        return min(120, remaining)

    def download(self, path, destination, expected=None, maximum=MAX_DOWNLOAD):
        """Stream a GitHub archive or release asset without forwarding credentials."""
        if not re.fullmatch(r"/actions/artifacts/\d+/zip|/releases/assets/\d+", path):
            raise ValueError("Unsupported dataset download path")
        request = Request(f"https://api.github.com/repos/{self.repository}{path}", headers={
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json" if path.startswith("/actions/") else "application/octet-stream",
            "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "acousticTSbenchmark-dataset"})
        try:
            response = self.opener.open(request, timeout=self.timeout())
        except HTTPError as error:
            if error.code not in (301, 302, 303, 307, 308):
                raise APIError(error.code, "Dataset download failed") from None
            target = urlparse(error.headers.get("Location", ""))
            host = target.hostname or ""
            if (target.scheme != "https" or target.username or not
                    (host.endswith(".githubusercontent.com") or host.endswith(".blob.core.windows.net") or
                     host.endswith(".actions.githubusercontent.com"))):
                raise ValueError("Unsupported dataset storage host")
            response = self.opener.open(Request(target.geturl()), timeout=self.timeout())
        target = Path(destination)
        temporary = target.with_suffix(target.suffix + ".part")
        digest, size = hashlib.sha256(), 0
        try:
            with response, temporary.open("wb") as output:
                while block := response.read(1024 * 1024):
                    self.timeout()
                    size += len(block)
                    if size > maximum:
                        raise ValueError("Dataset download exceeds its size limit")
                    output.write(block)
                    digest.update(block)
            actual = digest.hexdigest()
            if expected and actual != expected:
                raise ValueError("Dataset download checksum mismatch")
            temporary.replace(target)
            return actual
        finally:
            temporary.unlink(missing_ok=True)

    def upload(self, release_id, path, name):
        path = Path(path)
        if not ASSET_NAME.fullmatch(name) or not 0 < path.stat().st_size <= MAX_DOWNLOAD:
            raise ValueError("Invalid dataset asset name or size")
        url = f"https://uploads.github.com/repos/{self.repository}/releases/{int(release_id)}/assets?" + urlencode({"name": name})
        with path.open("rb") as source:
            request = Request(url, data=source, method="POST", headers={
                "Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github+json",
                "Content-Type": "application/gzip", "Content-Length": str(path.stat().st_size),
                "User-Agent": "acousticTSbenchmark-dataset"})
            try:
                with self.opener.open(request, timeout=self.timeout()) as response:
                    return json.loads(self.read(response, 1024 * 1024))
            except HTTPError as error:
                raise APIError(error.code, "Dataset upload failed") from None


def read_catalog(api):
    try:
        response = api.request("GET", f"/contents/{CATALOG}?ref={BRANCH}")
    except APIError as error:
        if error.status == 404:
            return None, None
        raise
    if response.get("encoding") != "base64" or response.get("size", 0) > MAX_CATALOG:
        raise ValueError("Invalid dataset catalog encoding or size")
    raw = base64.b64decode(response["content"], validate=False)
    if len(raw) > MAX_CATALOG:
        raise ValueError("Dataset catalog is too large")
    value = json.loads(raw)
    if value.get("schema_version") != 1 or value.get("repository") != api.repository:
        raise ValueError("Dataset catalog has an incompatible schema or repository")
    return value, response["sha"]


def write_catalog(api, value, previous_sha):
    body = dict(message="Update consolidated benchmark dataset", branch=BRANCH,
                content=base64.b64encode(json.dumps(value, allow_nan=False, separators=(",", ":")).encode()).decode())
    if previous_sha:
        body["sha"] = previous_sha
    for attempt in range(3):
        try:
            api.request("PUT", f"/contents/{CATALOG}", body)
            return
        except APIError as error:
            if error.status != 409 or attempt == 2:
                raise
            _, current_sha = read_catalog(api)
            if current_sha != previous_sha:
                raise ValueError("Dataset changed concurrently. Refusing to overwrite it") from None


def list_artifacts(api):
    entries = []
    for page in range(1, 101):
        response = api.request("GET", f"/actions/artifacts?per_page=100&page={page}")
        entries.extend(response["artifacts"])
        if len(response["artifacts"]) < 100:
            return entries
    raise ValueError("Artifact inventory exceeds the supported page count")


def select_artifacts(artifacts):
    """Keep final tables and reference joins, plus the last checkpoint of each attempt."""
    selected, checkpoints, baselines, full = [], {}, {}, set()
    for artifact in artifacts:
        if artifact.get("expired"):
            continue
        run = artifact.get("workflow_run", {}).get("id")
        if type(run) is not int:
            continue
        name = artifact["name"]
        match = TIMING_NAME.fullmatch(name)
        if match:
            kind, platform = match.groups()
            key = (run, platform)
            if kind == "retained":
                baselines[key] = artifact
            else:
                selected.append(artifact)
                if kind == "results":
                    full.add(key)
            continue
        match = CHECKPOINT_NAME.fullmatch(name)
        if match:
            platform, attempt, _ = match.groups()
            key = (run, platform, int(attempt))
            if key not in checkpoints or artifact["created_at"] > checkpoints[key]["created_at"]:
                checkpoints[key] = artifact
            full.add((run, platform))
        elif REFERENCE_NAME.fullmatch(name):
            selected.append(artifact)
    selected.extend(checkpoints.values())
    selected.extend(value for key, value in baselines.items() if key not in full)
    return sorted(selected, key=lambda item: (item["created_at"], item["id"]), reverse=True)


def restore_database(api, catalog, directory):
    database = directory / "benchmarks.sqlite"
    if catalog:
        asset = catalog["assets"]["database"]
        compressed = directory / "previous.sqlite.gz"
        api.download(f"/releases/assets/{int(asset['id'])}", compressed, asset["sha256"])
        temporary = database.with_suffix(".sqlite.tmp")
        try:
            with gzip.open(compressed, "rb") as source, temporary.open("wb") as output:
                size = 0
                while block := source.read(1024 * 1024):
                    size += len(block)
                    if size > MAX_DATABASE:
                        raise ValueError("Dataset database exceeds its size limit")
                    output.write(block)
            temporary.replace(database)
        finally:
            temporary.unlink(missing_ok=True)
            compressed.unlink(missing_ok=True)
    elif database.exists():
        raise ValueError("Uncatalogued local database already exists")
    connection = connect_database(database)
    if connection.execute("PRAGMA quick_check").fetchone()[0] != "ok":
        raise ValueError("Dataset database failed its integrity check")
    return connection, database


def ensure_release(api):
    try:
        return api.request("GET", f"/releases/tags/{RELEASE_TAG}")
    except APIError as error:
        if error.status != 404:
            raise
    return api.request("POST", "/releases", dict(tag_name=RELEASE_TAG, target_commitish="main",
        name="Consolidated benchmark dataset", make_latest="false",
        body="Benchmark data. Initial import in progress."))


def publish_dataset(api, connection, database, catalog, previous_sha, processed, errors, remaining):
    connection.commit()
    summary = database_summary(connection)
    if connection.execute("PRAGMA quick_check").fetchone()[0] != "ok":
        raise ValueError("Updated dataset failed its integrity check")
    generation = f"{int(time.time())}-{secrets.token_hex(4)}"
    release = ensure_release(api)
    files = {"database": database.with_suffix(".sqlite.gz"),
             "measurements": database.with_name("measurements.csv.gz"),
             "comparisons": database.with_name("comparisons.csv.gz")}
    with database.open("rb") as source, gzip.open(files["database"], "wb", compresslevel=6) as output:
        shutil.copyfileobj(source, output, 1024 * 1024)
    export_csv(connection, files["measurements"])
    export_comparisons_csv(connection, files["comparisons"])
    assets = {}
    for key, path in files.items():
        asset = api.upload(release["id"], path, f"dataset-{generation}-{path.name}")
        assets[key] = dict(id=asset["id"], url=asset["browser_download_url"],
                           bytes=path.stat().st_size, sha256=sha256(path))
    value = dict(schema_version=1, repository=api.repository, generation=generation, updated_at=utc_now(),
        release_url=release["html_url"], assets=assets, summary=summary, processed=processed,
        pending_artifacts=remaining, import_errors=errors,
        previous_assets=(catalog or {}).get("assets", {}))
    # Publish the pointer only after every immutable asset has uploaded. The
    # prior complete dataset stays downloadable if any earlier step fails.
    write_catalog(api, value, previous_sha)
    body = (f"Updated {value['updated_at']}. **{summary['measurements']:,} measurements**.\n\n"
            f"[SQLite database (.gz)]({assets['database']['url']}) · "
            f"[Measurements CSV (.gz)]({assets['measurements']['url']}) · "
            f"[Comparisons CSV (.gz)]({assets['comparisons']['url']})\n\n"
            f"Imports: {summary['artifacts']} complete, {remaining} pending, {len(errors)} errors.")
    api.request("PATCH", f"/releases/{release['id']}", dict(body=body))
    keep = {asset["id"] for group in (assets, value["previous_assets"]) for asset in group.values()}
    # Only this feature's obsolete snapshots on its dedicated release are
    # removed. Every observation remains inside each cumulative database.
    obsolete = []
    for page in range(1, 11):
        candidates = api.request("GET", f"/releases/{release['id']}/assets?per_page=100&page={page}")
        for asset in candidates:
            if asset["id"] not in keep and ASSET_NAME.fullmatch(asset["name"]):
                obsolete.append(asset["id"])
        if len(candidates) < 100:
            break
    for asset_id in obsolete:
        api.request("DELETE", f"/releases/assets/{asset_id}")
    return value


def synchronize(api, directory, maximum=8):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    catalog, previous_sha = read_catalog(api)
    processed = dict((catalog or {}).get("processed", {}))
    candidates = [item for item in select_artifacts(list_artifacts(api)) if str(item["id"]) not in processed]
    if not candidates:
        return dict(changed=False, pending_artifacts=0, pending_untried=0, import_errors=[], batch_errors=[])
    errors_by_id = {item["artifact_id"]: item for item in (catalog or {}).get("import_errors", [])
                    if any(candidate["id"] == item["artifact_id"] for candidate in candidates)}
    candidates.sort(key=lambda item: item["id"] in errors_by_id)
    connection, database = restore_database(api, catalog, directory)
    imported, batch_errors = 0, []
    try:
        for artifact in candidates[:maximum]:
            path = directory / "incoming.zip"
            try:
                expected = artifact.get("digest") or ""
                expected = expected.removeprefix("sha256:") if expected.startswith("sha256:") else None
                digest = api.download(f"/actions/artifacts/{artifact['id']}/zip", path, expected)
                result = import_artifact(connection, path, dict(artifact, archive_sha256=digest), api.repository)
                processed[str(artifact["id"])] = dict(name=artifact["name"], run_id=artifact["workflow_run"]["id"], sha256=digest)
                errors_by_id.pop(artifact["id"], None)
                imported += 1
                print(json.dumps(result), flush=True)
            except (ValueError, OSError, RuntimeError) as error:
                errors_by_id[artifact["id"]] = dict(artifact_id=artifact["id"], name=artifact["name"], error=str(error)[:300])
                batch_errors.append(errors_by_id[artifact["id"]])
                print(f"Artifact {artifact['id']} was not imported: {type(error).__name__}: {str(error)[:200]}", file=sys.stderr, flush=True)
            finally:
                path.unlink(missing_ok=True)
        remaining = sum(str(item["id"]) not in processed for item in candidates)
        untried = sum(str(item["id"]) not in processed and item["id"] not in errors_by_id for item in candidates)
        errors = list(errors_by_id.values())
        if imported or not catalog:
            value = publish_dataset(api, connection, database, catalog, previous_sha, processed, errors, remaining)
            return dict(changed=True, pending_artifacts=remaining, pending_untried=untried, import_errors=errors, batch_errors=batch_errors, summary=value["summary"],
                        release_url=value["release_url"])
        write_catalog(api, dict(catalog, import_errors=errors, pending_artifacts=remaining), previous_sha)
        return dict(changed=False, pending_artifacts=remaining, pending_untried=untried, import_errors=errors, batch_errors=batch_errors)
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--max-artifacts", type=int, default=8)
    args = parser.parse_args()
    if not 1 <= args.max_artifacts <= 40:
        parser.error("--max-artifacts must be between 1 and 40")
    api = DatasetAPI(args.repository, os.environ.get("GITHUB_TOKEN"), timeout_seconds=1100)
    result = synchronize(api, args.directory, args.max_artifacts)
    print(json.dumps(result), flush=True)
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            output.write(f"pending={result['pending_artifacts']}\n")
            output.write(f"pending_untried={result['pending_untried']}\n")
            output.write(f"changed={str(result['changed']).lower()}\n")
    if result["batch_errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
