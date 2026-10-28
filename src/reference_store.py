"""Persist qualified numerical references on a Git branch and join timing artifacts."""

from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import sys
import zipfile

from benchmark import ROOT, atomic_json, export, load_references, read_json, utc_now
from publish_progress import APIError, GitHub, PLATFORMS, comparison_rows, select_artifact
from references import build_requests, cached_reference, export_references, valid_cached

BRANCH = "reference-data"
MAX_RECORD_BYTES = 2 * 1024 * 1024


def branch_tree(api, branch=BRANCH):
    if branch not in (BRANCH, "local-reference-data"):
        raise ValueError("Unsupported numerical reference branch")
    try:
        head = api.request("GET", f"/git/ref/heads/{branch}")["object"]["sha"]
    except APIError as error:
        if error.status == 404:
            return None, None, {}
        raise
    commit = api.request("GET", f"/git/commits/{head}")
    tree_sha = commit["tree"]["sha"]
    tree = api.request("GET", f"/git/trees/{tree_sha}?recursive=1")
    if tree.get("truncated"):
        raise ValueError("Reference tree exceeds GitHub's recursive listing limit")
    return head, tree_sha, {entry["path"]: entry["sha"] for entry in tree["tree"]
                            if entry["type"] == "blob"}


def blob_record(api, sha):
    blob = api.request("GET", f"/git/blobs/{sha}")
    if blob.get("encoding") != "base64" or blob.get("size", 0) > MAX_RECORD_BYTES:
        raise ValueError("Unsupported reference blob")
    raw = base64.b64decode("".join(blob["content"].split()), validate=True)
    if len(raw) > MAX_RECORD_BYTES:
        raise ValueError("Oversized reference blob")
    actual = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
    if actual != sha:
        raise ValueError("Reference blob content does not match Git SHA")
    return json.loads(raw)


def restore_cache(api, jobs, cache_dir):
    """Fetch only requested keys. Invalid evidence is a miss, network errors are failures."""
    head, _, tree = branch_tree(api)
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    wanted = {job["reference_id"]: job for job in jobs
              if cached_reference(cache_dir / f"{job['reference_id']}.json", job) is None
              and f"references/{job['reference_id']}.json" in tree}
    # One archive download avoids thousands of API requests for a full dense grid.
    if len(wanted) > 8:
        with zipfile.ZipFile(io.BytesIO(api.repository_archive(head))) as archive:
            for entry in archive.infolist():
                parts = PurePosixPath(entry.filename).parts
                if len(parts) != 3 or parts[1] != "references" or not parts[2].endswith(".json"):
                    continue
                identity = parts[2][:-5]
                if identity not in wanted or entry.file_size > MAX_RECORD_BYTES:
                    continue
                raw = archive.read(entry)
                sha = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
                if tree.get(f"references/{identity}.json") != sha:
                    continue
                try:
                    record = json.loads(raw)
                    if valid_cached(record, wanted[identity]):
                        atomic_json(cache_dir / f"{identity}.json", record)
                except ValueError:
                    pass
        restored = sum(cached_reference(cache_dir / f"{job['reference_id']}.json", job) is not None
                       for job in jobs)
        return dict(planned=len(jobs), cached=restored, missing=len(jobs) - restored)

    def restore(job):
        path = cache_dir / f"{job['reference_id']}.json"
        if cached_reference(path, job) is not None:
            return True
        sha = tree.get(f"references/{job['reference_id']}.json")
        if sha is None:
            return False
        try:
            record = blob_record(api, sha)
        except (ValueError, KeyError, TypeError):
            return False
        if not valid_cached(record, job):
            return False
        atomic_json(path, record)
        return True

    with ThreadPoolExecutor(max_workers=4) as pool:
        restored = sum(pool.map(restore, jobs))
    return dict(planned=len(jobs), cached=restored, missing=len(jobs) - restored)


def publish_cache(api, jobs, cache_dir, *, branch=BRANCH):
    """Commit new successful keys atomically, retaining concurrent writers' results."""
    records = {}
    by_id = {job["reference_id"]: job for job in jobs}
    for job in jobs:
        record = cached_reference(Path(cache_dir) / f"{job['reference_id']}.json", job)
        if record is not None:
            records[f"references/{job['reference_id']}.json"] = record
    if not records:
        return 0
    for _ in range(5):
        head, tree_sha, tree = branch_tree(api, branch)
        changes = []
        for path, record in records.items():
            serialized = json.dumps(record, allow_nan=False, separators=(",", ":"))
            raw = serialized.encode()
            sha = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
            if path in tree:
                if tree[path] == sha:
                    continue
                try:
                    previous = blob_record(api, tree[path])
                    if valid_cached(previous, by_id[record["reference_id"]]):
                        continue
                except (ValueError, KeyError, TypeError):
                    pass
            changes.append(dict(path=path, mode="100644", type="blob",
                                content=serialized))
        if not changes:
            return 0
        body = dict(tree=changes)
        if tree_sha:
            body["base_tree"] = tree_sha
        new_tree = api.request("POST", "/git/trees", body)
        commit = api.request("POST", "/git/commits", dict(
            message=f"Save {len(changes)} qualified numerical references",
            tree=new_tree["sha"], parents=[head] if head else []))
        try:
            if head:
                api.request("PATCH", f"/git/refs/heads/{branch}",
                            dict(sha=commit["sha"], force=False))
            else:
                api.request("POST", "/git/refs", dict(ref=f"refs/heads/{branch}", sha=commit["sha"]))
            return len(changes)
        except APIError as error:
            if error.status not in (409, 422):
                raise
    raise RuntimeError("Reference branch changed concurrently too often. Retry publication")


def join_directory(directory, references_path):
    """Refresh derived comparisons without running or modifying any timing measurements."""
    directory = Path(directory)
    rows = read_json(directory / "results.json")
    references = load_references(references_path)
    status = read_json(directory / "status.json")
    # Remove old derived fields before rejoining so missing references remain missing.
    cases = [{key: value for key, value in row.items()
              if not key.startswith("reference_") and key != "delta_ts_db"} for row in rows]
    records = {row["case_id"]: row for row in cases if row["status"] != "pending"}
    export(directory, cases, records, references, status["state"])
    atomic_json(directory / "references.json", read_json(references_path))
    return sum(row["status"] == "ok" and row["physical_id"] in references
               for row in comparison_rows(directory, rows, references=references))


def download_timing(api, run_id, platform, directory):
    artifacts = []
    for page in range(1, 101):
        part = api.request("GET", f"/actions/runs/{run_id}/artifacts?per_page=100&page={page}")["artifacts"]
        artifacts.extend(part)
        if len(part) < 100:
            break
    artifact = select_artifact(artifacts, platform)
    if artifact is None:
        return False
    from campaign import setup_only_checkpoint
    content = api.archive(artifact["id"])
    if setup_only_checkpoint(content, api.repository, run_id) is not None:
        print(f"No timing measurements for {platform}: setup did not produce a checkpoint", flush=True)
        return False
    from refresh import baseline_settings
    if baseline_settings(content, api.repository, run_id, platform) is not None:
        print(f"No new timing measurements for {platform}: refresh setup did not produce a checkpoint", flush=True)
        return False
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    values = {}
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        for entry in archive.infolist():
            name = PurePosixPath(entry.filename).name
            if name not in ("manifest.json", "results.json", "status.json", "retained-results.json"):
                continue
            if name in values or entry.file_size > 128 * 1024 * 1024:
                raise ValueError("Ambiguous or oversized timing artifact")
            values[name] = json.loads(archive.read(entry))
    if not {"manifest.json", "results.json", "status.json"} <= values.keys():
        raise ValueError("Incomplete timing artifact")
    if "retained-results.json" in values:
        from publish_progress import retained_comparison_rows
        retained_comparison_rows(values["retained-results.json"], values["results.json"])
    for name, value in values.items():
        atomic_json(directory / name, value)
    if "retained-results.json" not in values:
        (directory / "retained-results.json").unlink(missing_ok=True)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("restore", "publish", "join", "attach"))
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--config", default=ROOT / "benchmark.json")
    parser.add_argument("--sources-lock", default=ROOT / "sources.lock.json")
    parser.add_argument("--profile", default="pilot")
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--run-id", type=int)
    parser.add_argument("--platform", choices=PLATFORMS)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    args = parser.parse_args()
    jobs = build_requests(read_json(args.config), read_json(args.sources_lock), args.profile)
    if not 0 <= args.shard_index < args.shard_count:
        parser.error("Invalid reference shard")
    jobs = [job for job in jobs if int(job["physical_id"], 16) % args.shard_count == args.shard_index]
    api = None if args.action == "join" else GitHub(args.repository, os.environ.get("GITHUB_TOKEN"), 300)
    if args.action == "attach":
        if not args.run_id or not args.platform or not args.directory:
            parser.error("attach requires --run-id, --platform and --directory")
        if not download_timing(api, args.run_id, args.platform, args.directory):
            print(f"No timing artifact for {args.platform}")
            return
        manifest = read_json(args.directory / "manifest.json")
        jobs = manifest.get("reference_requests") or build_requests(
            manifest["configuration"], manifest["sources"], manifest["profile"])
    if args.action in ("restore", "attach"):
        status = restore_cache(api, jobs, args.cache_dir)
        print(json.dumps(status), flush=True)
        if args.action == "restore" and os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as stream:
                stream.write(f"missing={status['missing']}\n")
    if args.action == "publish":
        print(f"Persisted {publish_cache(api, jobs, args.cache_dir)} new references", flush=True)
    export_references(jobs, args.cache_dir, args.output)
    curve_run = args.run_id if args.action == "attach" else os.getenv("REFERENCE_TIMING_RUN_ID", os.getenv("GITHUB_RUN_ID"))
    if api is not None and curve_run:
        from reference_curves import curves_from_cache, publish_curves
        try:
            published = publish_curves(api, int(curve_run), curves_from_cache(jobs, args.cache_dir))
            print(f"Published {published} additional TS reference points", flush=True)
        except (OSError, RuntimeError, ValueError) as error:
            # Reference publication must not prevent cached values from being used.
            print(f"TS reference curve publication deferred ({type(error).__name__})", file=sys.stderr)
    if args.action in ("join", "attach"):
        if not args.directory:
            parser.error("join requires --directory")
        compared = join_directory(args.directory, args.output)
        print(f"Joined {compared} successful timing observations", flush=True)
    if args.action == "attach":
        from publish_progress import compact_payload, publish
        run = api.request("GET", f"/actions/runs/{args.run_id}")
        payload = compact_payload(args.repository, args.platform, args.run_id, run["run_attempt"],
            comparison_rows(args.directory, read_json(args.directory / "results.json")),
            read_json(args.directory / "status.json"), "completed" if run.get("status") == "completed" else "running")
        comparison_run = os.getenv("GITHUB_RUN_ID")
        if comparison_run:
            payload["artifacts_url"] = f"https://github.com/{args.repository}/actions/runs/{comparison_run}#artifacts"
        publish(api, payload)


if __name__ == "__main__":
    main()
