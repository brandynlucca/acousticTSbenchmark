"""Resume a finite local reference plan and publish only qualified saved results."""
from __future__ import annotations

import argparse
from contextlib import AbstractContextManager
import hashlib
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import tomllib

import psutil

from benchmark import ROOT, atomic_json, fingerprint, read_json, utc_now
from publish_progress import GitHub, safe_error_summary
import reference_curves
import reference_store
import references

PREFIX = "local-reference-generation"
DEFAULT_WORK = ROOT / ".scratch/codex/dense-benchmark"
SOURCE = Path("C:/Users/Brandyn/GitHub/AcousticScattering.jl")
JULIA = Path("C:/Users/Brandyn/AppData/Local/Programs/Julia-1.12.1/bin/julia.exe")
THREADS = 4
CASE_TIMEOUT = 600
MINIMUM_AVAILABLE = 3 * 1024 ** 3


def file_hash(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def git_output(root, *arguments):
    result = subprocess.run(["git", "-c", f"safe.directory={Path(root).as_posix()}",
        "-C", str(root), *arguments], capture_output=True, text=True, check=True)
    return result.stdout.strip()


def numerical_files(root):
    root = Path(root)
    files = {p.resolve() for name in ("Project.toml", "Manifest.toml", "Artifacts.toml")
             if (p := root / name).is_file()}
    for name in ("src", "ext", "deps"):
        directory = root / name
        if directory.is_dir():
            files.update(p.resolve() for p in directory.rglob("*")
                         if p.is_file() and "__pycache__" not in p.parts)
    return files


def source_guard(source, project, julia=JULIA):
    source, project = Path(source).resolve(), Path(project).resolve()
    manifest = tomllib.loads((project / "Manifest.toml").read_text(encoding="utf-8"))
    package_paths = {source}
    for entries in manifest.get("deps", {}).values():
        for entry in entries:
            if "path" in entry:
                package_paths.add((project / entry["path"]).resolve())
    files = {project / "Project.toml", project / "Manifest.toml", Path(julia).resolve()}
    if (project / "LocalPreferences.toml").exists():
        files.add(project / "LocalPreferences.toml")
    heads = {}
    for package in sorted(package_paths):
        files.update(numerical_files(package))
        heads[str(package)] = dict(sha=git_output(package, "rev-parse", "HEAD"),
            numerical_dirty=bool(git_output(package, "status", "--porcelain", "--",
                "src", "ext", "deps", "Project.toml", "Manifest.toml", "Artifacts.toml")))
    for name in ("references.py", "worker_reference.jl", "worker_julia.jl", "process_control.py",
                 "process_gate.py", "local_references.py", "reference_store.py", "reference_curves.py",
                 "publish_progress.py", "benchmark.py"):
        files.add(ROOT / "src" / name)
    return dict(package_roots=[str(p) for p in sorted(package_paths)], source_repo=str(source),
        project=str(project), julia_executable=str(Path(julia).resolve()), heads=heads,
        file_sha256={str(p): file_hash(p) for p in sorted(files)})


def verify_guard(saved):
    current = source_guard(saved["source_repo"], saved["project"], saved["julia_executable"])
    if current != saved:
        raise ValueError("Numerical sources, Julia environment or reference controller changed since planning")


def priority(job):
    return (job["aspect_ratio"] != 50, 0 if job["aspect_ratio"] == 50 else job["aspect_ratio"],
            job["frequency_hz"], ("rigid", "pressure_release", "liquid", "gas").index(job["boundary"]),
            job["incidence_deg"])


def selected_jobs(configuration, profile, source_sha, known, adapters):
    known_ids = set(reference_curves._validated(known))
    jobs = references.reference_jobs(configuration, profile, source_sha, THREADS, adapters=adapters)
    todo = sorted((job for job in jobs if job["physical_id"] not in known_ids), key=priority)
    return todo, len(jobs), sorted({job["physical_id"] for job in jobs} & known_ids)


def paths(work):
    work = Path(work).resolve()
    stages, cache = work / "julia-smoke-rigid", work / "python-smoke"
    if not all(path.is_dir() for path in (work, stages, cache)):
        raise ValueError("Work, julia-smoke-rigid and python-smoke directories must already exist")
    return dict(work=work, stages=stages, cache=cache,
        manifest=work / f"{PREFIX}-manifest.json", status=work / f"{PREFIX}-status.json",
        lock=work / f"{PREFIX}.lock", stop=work / f"{PREFIX}.stop")


class ControllerLock(AbstractContextManager):
    """An OS-held lock prevents duplicate controllers, including after a crash."""
    def __init__(self, path):
        self.path, self.stream = Path(path), None

    def __enter__(self):
        self.stream = self.path.open("a+b")
        self.stream.seek(0, os.SEEK_END)
        if self.stream.tell() == 0:
            self.stream.write(b" ")
            self.stream.flush()
        self.stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.stream.close()
            self.stream = None
            raise RuntimeError("Another local reference controller owns this workspace") from None
        return self

    def __exit__(self, *args):
        if self.stream is not None:
            self.stream.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.stream.fileno(), fcntl.LOCK_UN)
            self.stream.close()
        return False


def create_plan(args):
    location = paths(args.work)
    with ControllerLock(location["lock"]):
        if location["manifest"].exists():
            raise ValueError("A local reference plan already exists. Resume it instead of replacing its history")
        configuration = read_json(args.config)
        existing = read_json(args.known_references)
        if existing.get("schema_version") != 1:
            raise ValueError("Known references require a compact schema-version-1 snapshot")
        project = Path(args.project or location["work"] / "julia").resolve()
        guard = source_guard(args.source_repo, project, args.julia)
        source_sha = guard["heads"][str(Path(args.source_repo).resolve())]["sha"]
        adapters = dict(references.adapter_signature(), local_source_snapshot=fingerprint(guard))
        jobs, planned, skipped = selected_jobs(configuration, args.profile, source_sha,
            existing["reference_curves"], adapters)
        runtime = dict(executables={"acousticscattering": [str(Path(args.julia).resolve()),
            "--startup-file=no", "--history-file=no", f"--project={project}"]},
            source_shas={"acousticscattering": source_sha},
            availability={"acousticscattering": {"status": "ready"}}, env={},
            environment_fingerprint=fingerprint(guard), environment_compatibility={"julia_environments": {
                "acousticscattering": {"Manifest.toml": (project / "Manifest.toml").read_text(encoding="utf-8")}}})
        identity = dict(schema_version=1, repository=args.repository, run_id=args.run_id,
            configuration=configuration, profile=args.profile, threads=THREADS,
            case_timeout_seconds=CASE_TIMEOUT, minimum_available_bytes=MINIMUM_AVAILABLE,
            source_guard=guard, runtime=runtime, requests=jobs, planned=planned,
            skipped_existing_physical_ids=skipped, known_references_sha256=file_hash(args.known_references),
            configuration_input_sha256=file_hash(args.config))
        manifest = dict(identity, fingerprint=fingerprint(identity), created_at=utc_now())
        atomic_json(location["manifest"], manifest)
        atomic_json(location["status"], dict(schema_version=1, state="planned", updated_at=utc_now(),
            manifest_fingerprint=manifest["fingerprint"], planned=planned, skipped_existing=len(skipped),
            selected=len(jobs), attempted={}, published=[], publication_errors=[], history=[]))
        return manifest


def validate_manifest(manifest):
    identity = {k: v for k, v in manifest.items() if k not in ("fingerprint", "created_at")}
    if fingerprint(identity) != manifest.get("fingerprint"):
        raise ValueError("Local reference plan fingerprint differs")
    if (manifest.get("threads") != THREADS or manifest.get("case_timeout_seconds") != CASE_TIMEOUT
            or manifest.get("minimum_available_bytes") != MINIMUM_AVAILABLE):
        raise ValueError("Local reference resource limits changed")
    identifiers = [job["reference_id"] for job in manifest["requests"]]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("Duplicate planned reference IDs")
    for job in manifest["requests"]:
        if fingerprint(job["signature"]) != job["reference_id"]:
            raise ValueError("Invalid planned reference signature")


def provenance(manifest):
    return dict(origin="local_reference_generation", source_sha=manifest["runtime"]["source_shas"]["acousticscattering"],
        source_guard_sha256=fingerprint(manifest["source_guard"]), source_file_sha256=manifest["source_guard"]["file_sha256"],
        source_heads=manifest["source_guard"]["heads"], environment_fingerprint=manifest["runtime"]["environment_fingerprint"],
        environment_compatibility=references.julia_provenance(manifest["runtime"]),
        plan_fingerprint=manifest["fingerprint"], threads=THREADS, blas_threads=1)


def record_evidence(job, evidence, location, metadata):
    record = references.reference_record(job, evidence, metadata)
    atomic_json(location["stages"] / f"{job['reference_id']}.record.json", record)
    if record["qualified"]:
        atomic_json(location["cache"] / f"{job['reference_id']}.json", record)
    return record


def restore_records(jobs, location, metadata):
    restored = {}
    for job in jobs:
        identity = job["reference_id"]
        record_path = location["stages"] / f"{identity}.record.json"
        attempt_path = location["stages"] / f"{identity}.json"
        cached = references.cached_reference(location["cache"] / f"{identity}.json", job)
        if record_path.exists():
            record = read_json(record_path)
            if (record.get("reference_id") != identity or record.get("signature") != job["signature"]
                    or record.get("physical_id") != job["physical_id"]):
                raise ValueError("Recorded reference identity differs from the saved plan")
            rebuilt = references.reference_record(job, record["evidence"], record.get("provenance"))
            if any(record.get(key) != rebuilt.get(key) for key in
                   ("qualified", "status", "ts_db", "qualification", "amplitude_real_m", "amplitude_imag_m")):
                raise ValueError("Recorded reference qualification differs from retained evidence")
            if record["qualified"] and cached is None:
                atomic_json(location["cache"] / f"{identity}.json", record)
            restored[identity] = {key: record[key] for key in ("status", "qualified", "physical_id")}
        elif cached is not None:
            restored[identity] = {key: cached[key] for key in ("status", "qualified", "physical_id")}
        elif attempt_path.exists():
            record = record_evidence(job, read_json(attempt_path), location, metadata)
            restored[identity] = {key: record[key] for key in ("status", "qualified", "physical_id")}
    return restored


def github_api(repository):
    result = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, check=True)
    return GitHub(repository, result.stdout.strip(), timeout_seconds=300)


def publish_records(manifest, jobs, cache, api_factory=github_api):
    # Every network operation gets a fresh deadline. Credentials remain in memory.
    api = api_factory(manifest["repository"])
    reference_store.publish_cache(api, jobs, cache, branch="local-reference-data")
    curves = reference_curves.curves_from_cache(jobs, cache)
    api = api_factory(manifest["repository"])
    reference_curves.publish_shared_curves(api, curves)
    api = api_factory(manifest["repository"])
    reference_curves.publish_curves(api, manifest["run_id"], curves)


class Publisher:
    """A single network thread keeps publication outside worker supervision."""
    def __init__(self, manifest, location, state, state_lock, save, publish=publish_records):
        self.manifest, self.location, self.state = manifest, location, state
        self.lock, self.save, self.publish = state_lock, save, publish
        self.by_id = {job["reference_id"]: job for job in manifest["requests"]}
        self.last_attempt = 0.
        self.last_failed = False
        self.stopping = threading.Event()
        self.thread = None

    def pending(self):
        with self.lock:
            published = set(self.state["published"])
            return [identity for identity, record in self.state["attempted"].items()
                    if record["qualified"] and identity not in published]

    def step(self, force=False):
        pending = self.pending()
        if not pending or not force and (len(pending) < 10 or self.last_failed) and time.monotonic() - self.last_attempt < 60:
            return False
        if not force and time.monotonic() - self.last_attempt < 5:
            return False
        self.last_attempt = time.monotonic()
        selected = pending[:10]
        try:
            self.publish(self.manifest, [self.by_id[i] for i in selected], self.location["cache"])
        except Exception as error:
            with self.lock:
                self.state["publication_errors"].append(dict(at=utc_now(), error=safe_error_summary(error)))
                self.state["publication_errors"] = self.state["publication_errors"][-20:]
                self.save()
            # Avoid tight retries when a full batch remains pending.
            self.last_attempt = time.monotonic()
            self.last_failed = True
            return False
        self.last_failed = False
        with self.lock:
            self.state["published"] = sorted(set(self.state["published"]) | set(selected))
            self.state["last_published_at"] = utc_now()
            self.save()
        return True

    def start(self):
        def loop():
            while not self.stopping.wait(1):
                self.step()
        self.thread = threading.Thread(target=loop, name="local-reference-publisher", daemon=True)
        self.thread.start()

    def finish(self):
        self.stopping.set()
        if self.thread is not None:
            self.thread.join()
        # Flush remaining bounded batches. A failed upload remains durable and retryable.
        while self.pending() and self.step(force=True):
            pass


class PauseRequested(Exception):
    pass


def execute(manifest, location, *, resume=False, runner=references._run_batch,
            verify=verify_guard, available=lambda: psutil.virtual_memory().available,
            sleep=time.sleep, publish=publish_records):
    validate_manifest(manifest)
    verify(manifest["source_guard"])
    state = read_json(location["status"])
    if state.get("manifest_fingerprint") != manifest["fingerprint"]:
        raise ValueError("Local reference checkpoint belongs to a different plan")
    if not resume and state.get("state") != "planned":
        raise ValueError("Use --resume to continue an existing local reference checkpoint")
    jobs, metadata = manifest["requests"], provenance(manifest)
    restored = restore_records(jobs, location, metadata)
    state_lock = threading.RLock()
    state["attempted"] = {identity: dict(status=r["status"], qualified=r["qualified"],
        physical_id=r["physical_id"]) for identity, r in restored.items()}
    state.setdefault("published", [])
    if not set(state["published"]) <= {identity for identity, record in restored.items() if record["qualified"]}:
        raise ValueError("Publication checkpoint lacks corresponding qualified local records")
    state.setdefault("publication_errors", [])
    state.setdefault("history", []).append(dict(at=utc_now(), action="resume" if resume else "start",
        previous_state=state.get("state"), recovered=len(restored)))
    state["controller_pid"] = os.getpid()
    state["consecutive_worker_failures"] = 0
    del restored

    def save():
        with state_lock:
            state["updated_at"] = utc_now()
            state["attempted_count"] = len(state["attempted"])
            state["qualified"] = sum(r["qualified"] for r in state["attempted"].values())
            state["failed"] = state["attempted_count"] - state["qualified"]
            state["remaining"] = len(jobs) - state["attempted_count"]
            state["publication_pending"] = state["qualified"] - len(state["published"])
            atomic_json(location["status"], state)

    def accept(job, evidence):
        record = record_evidence(job, evidence, location, metadata)
        with state_lock:
            state["attempted"][job["reference_id"]] = dict(status=record["status"],
                qualified=record["qualified"], physical_id=job["physical_id"])
            state["current"] = None
            save()
        print(f"LOCAL_REFERENCE {job['reference_id']} {record['status']} qualified={record['qualified']}", flush=True)
        if location["stop"].exists():
            raise PauseRequested()

    def progress(active):
        with state_lock:
            state["current"] = active
            state["state"] = "running"
            state["pause_requested"] = location["stop"].exists()
            save()

    publisher = Publisher(manifest, location, state, state_lock, save, publish=publish)
    publisher.start()
    try:
        while True:
            todo = [job for job in jobs if job["reference_id"] not in state["attempted"]]
            if location["stop"].exists():
                state["state"] = "paused"
                break
            if not todo:
                state["state"] = "completed"
                break
            if available() < manifest["minimum_available_bytes"]:
                state["state"] = "waiting_for_memory"
                save()
                sleep(15)
                continue
            verify(manifest["source_guard"])
            state["state"] = "running"
            save()
            before = set(state["attempted"])
            deadline = time.time() + (len(todo) + 1) * manifest["case_timeout_seconds"]
            runner(todo, manifest["runtime"], location["stages"], THREADS, deadline, CASE_TIMEOUT,
                accept, progress, attempts_dir=location["stages"])
            added = [state["attempted"][key] for key in set(state["attempted"]) - before]
            failed_worker = not added or all(r["status"] in ("worker_error", "setup_error") for r in added)
            state["consecutive_worker_failures"] = state["consecutive_worker_failures"] + 1 if failed_worker else 0
            if state["consecutive_worker_failures"] >= 2:
                state["state"] = "stopped_after_worker_failures"
                break
            save()
    except PauseRequested:
        state["state"] = "paused"
    except BaseException as error:
        state["state"] = "stopped_after_error"
        state["last_error"] = safe_error_summary(error)
        save()
        raise
    finally:
        publisher.finish()
        state["current"] = None
        if state.get("state") == "completed" and publisher.pending():
            state["state"] = "completed_publication_pending"
        save()
    return state


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    action = result.add_mutually_exclusive_group(required=True)
    action.add_argument("--plan", action="store_true")
    action.add_argument("--run", action="store_true")
    action.add_argument("--resume", action="store_true")
    result.add_argument("--work", type=Path, default=DEFAULT_WORK)
    result.add_argument("--config", type=Path)
    result.add_argument("--known-references", type=Path)
    result.add_argument("--source-repo", type=Path, default=SOURCE)
    result.add_argument("--project", type=Path)
    result.add_argument("--julia", type=Path, default=JULIA)
    result.add_argument("--profile", default="full")
    result.add_argument("--repository", default="brandynlucca/acousticTSbenchmark")
    result.add_argument("--run-id", type=int, default=40737192500)
    return result


def main():
    args = parser().parse_args()
    if args.plan:
        if not args.config or not args.known_references:
            raise ValueError("Planning requires --config and --known-references")
        manifest = create_plan(args)
        print(json.dumps(dict(manifest=str(paths(args.work)["manifest"]), planned=manifest["planned"],
            selected=len(manifest["requests"]), skipped_existing=len(manifest["skipped_existing_physical_ids"]))))
        return
    location = paths(args.work)
    with ControllerLock(location["lock"]):
        state = execute(read_json(location["manifest"]), location, resume=args.resume)
    print(json.dumps({key: state[key] for key in ("state", "attempted_count", "qualified", "remaining", "publication_pending")}))


if __name__ == "__main__":
    main()
