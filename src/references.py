"""Build and reuse independently checked BEM and FEM-BEM reference calculations."""

from __future__ import annotations

import argparse
import hashlib
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import tomllib

from benchmark import (BOUNDARIES, PHYSICAL_FIELDS, ROOT, atomic_json, build_cases,
                       fingerprint, physical_identity, positive, read_json, utc_now)


POLICY = dict(version=1, spatial_tolerance_db=0.005, fourier_tolerance_db=0.005,
              relative_complex_tolerance=0.001, amplitude_floor_m=1e-10,
              mode_increment=8)


def source_digest(path):
    return hashlib.sha256(Path(path).read_text(encoding="utf-8").replace("\r\n", "\n").encode("utf-8")).hexdigest()


def adapter_signature():
    return {name: source_digest(ROOT / "src" / name)
            for name in ("references.py", "worker_reference.jl", "worker_julia.jl")}


def julia_provenance(runtime):
    """Retain exact dependency identities without repeated paths or dependency edges."""
    environment = runtime.get("environment_compatibility", {}).get("julia_environments", {}).get("acousticscattering", {})
    manifest = environment.get("Manifest.toml", {})
    if isinstance(manifest, str):
        manifest = tomllib.loads(manifest)
    preferences = environment.get("LocalPreferences.toml", {})
    if isinstance(preferences, str):
        preferences = tomllib.loads(preferences)
    packages = {}
    for name, entries in manifest.get("deps", {}).items():
        packages[name] = [{key: entry[key] for key in ("uuid", "version", "git-tree-sha1", "repo-url", "repo-rev")
                          if key in entry} for entry in entries]
    return dict(julia_version=manifest.get("julia_version"), packages=packages,
                preferences=preferences, source_sha=runtime.get("source_shas", {}).get("acousticscattering"))


def reference_jobs(configuration, profile, source_sha, threads=4, adapters=None):
    """Identify reference physics independently of timing software and repetition."""
    if not isinstance(threads, int) or threads < 1:
        raise ValueError("threads must be a positive integer")
    if len(source_sha) != 40 or any(c not in "0123456789abcdef" for c in source_sha):
        raise ValueError("An exact AcousticScattering source SHA is required")
    adapters = adapter_signature() if adapters is None else adapters
    jobs = []
    for case in build_cases(configuration, profile, ["acousticscattering"], 1):
        boundary = case["boundary"]
        requested = case["controls"]
        controls = dict(adaptive=True, target_tol=POLICY["spatial_tolerance_db"],
                        max_refinements=8, rtol=1e-6, quadrature_extra=0, chief_points=8,
                        boundary_order=5 if boundary == "gas" else 7)
        if boundary in ("gas", "liquid"):
            controls["order"] = 6 if boundary == "gas" else 8
        controls.update({k: v for k, v in requested.items() if k not in BOUNDARIES})
        controls.update(requested.get(boundary, {}))
        controls["mode_threads"] = threads
        if controls["adaptive"] is not True or controls["target_tol"] > POLICY["spatial_tolerance_db"]:
            raise ValueError("References require adaptive spatial checks at the qualification tolerance")
        positive(controls["target_tol"], "target_tol")
        physical = {key: (float(case[key]) if case[key] is not None and key != "boundary"
                          else case[key]) for key in PHYSICAL_FIELDS}
        signature = dict(schema_version=1, physical=physical, controls=controls,
                         method="fem_bem" if boundary in ("gas", "liquid") else "bem",
                         source_sha=source_sha, adapters=adapters, policy=dict(POLICY))
        jobs.append(dict(physical, reference_id=fingerprint(signature), signature=signature,
                         physical_id=physical_identity(physical), software="acousticscattering",
                         source_sha=source_sha, method=signature["method"], controls=controls,
                         policy=dict(POLICY), aspect_ratio=case["aspect_ratio"]))
    # Nearby angles can share compiled code. No private numerical cache is enabled.
    return sorted(jobs, key=lambda j: (j["boundary"], j["aspect_ratio"],
                                      j["frequency_hz"], j["incidence_deg"]))


def build_requests(configuration, lock, profile, threads=4):
    return reference_jobs(configuration, profile, lock["sources"]["acousticscattering"]["sha"], threads)


def _finite(value):
    return isinstance(value, (float, int)) and not isinstance(value, bool) and math.isfinite(value)


def _stage(stage, tolerance):
    if not isinstance(stage, dict):
        return None
    amplitude = stage.get("amplitude", {})
    if not isinstance(amplitude, dict) or not all(_finite(amplitude.get(k)) for k in ("real", "imag")):
        return None
    value = complex(amplitude["real"], amplitude["imag"])
    ts = stage.get("ts_db")
    if not _finite(ts) or abs(value) == 0 or abs(ts - 20 * math.log10(abs(value))) > 1e-8:
        return None
    diagnostics = stage.get("diagnostics", {})
    report = diagnostics.get("solver_report", {})
    refinement = report.get("refinement", {})
    change = refinement.get("change_db")
    modes = report.get("solver_options", {}).get("m_max")
    valid = (stage.get("status") == "ok" and diagnostics.get("spatial_converged") is True
             and refinement.get("converged") is True and _finite(change) and 0 <= change <= tolerance
             and report.get("converged", True) is not False
             and isinstance(modes, int) and not isinstance(modes, bool) and modes >= 0)
    return dict(amplitude=value, ts_db=ts, change_db=change, m_max=modes) if valid else None


def reference_record(job, evidence, provenance=None):
    """Derive qualification from retained solver output, never a supplied flag."""
    policy = job["policy"]
    baseline = _stage(evidence.get("baseline"), policy["spatial_tolerance_db"])
    expanded = _stage(evidence.get("expanded"), policy["spatial_tolerance_db"])
    axial = job["incidence_deg"] == 0
    difference_db = relative = absolute = None
    fourier_pass = False
    if baseline is not None:
        if axial:
            fourier_pass = baseline["m_max"] == 0 and evidence.get("expanded") is None
        elif expanded is not None:
            difference_db = abs(expanded["ts_db"] - baseline["ts_db"])
            absolute = abs(expanded["amplitude"] - baseline["amplitude"])
            relative = absolute / max(abs(expanded["amplitude"]), abs(baseline["amplitude"]),
                                      policy["amplitude_floor_m"])
            fourier_pass = (expanded["m_max"] == baseline["m_max"] + policy["mode_increment"]
                            and difference_db <= policy["fourier_tolerance_db"]
                            and relative <= policy["relative_complex_tolerance"])
    qualified = baseline is not None and fourier_pass
    chosen = baseline if axial else expanded
    qualification = dict(policy=policy, spatial_pass=baseline is not None and (axial or expanded is not None),
                         fourier_pass=fourier_pass,
                         fourier_check="exact axial symmetry, m=0" if axial else "increase m_max by 8",
                         fourier_change_db=difference_db, relative_complex_change=relative,
                         absolute_amplitude_change_m=absolute,
                         criterion="empirical spatial and Fourier convergence checks, not an absolute error bound",
                         uncertainty_kind="sum of spatial and Fourier acceptance tolerances")
    record = {key: job[key] for key in PHYSICAL_FIELDS}
    record.update(schema_version=1, reference_id=job["reference_id"], physical_id=job["physical_id"],
                  signature=job["signature"], source_sha=job["source_sha"], method=job["method"],
                  software="acousticscattering", controls=job["controls"], qualified=qualified,
                  status="ok" if qualified else evidence.get("status", "nonconverged"),
                  qualification=qualification, uncertainty_db=(policy["spatial_tolerance_db"] +
                      (0 if axial else policy["fourier_tolerance_db"])) if qualified else None,
                  ts_db=chosen["ts_db"] if chosen else None,
                  amplitude_real_m=chosen["amplitude"].real if chosen else None,
                  amplitude_imag_m=chosen["amplitude"].imag if chosen else None,
                  evidence=evidence, provenance=provenance or {}, finished_at=utc_now())
    if record["status"] == "ok" and not qualified:
        record["status"] = "nonconverged"
    return record


def valid_cached(record, job):
    """Validate identity and recompute qualification from the retained evidence."""
    try:
        if record.get("reference_id") != job["reference_id"] or record.get("signature") != job["signature"]:
            return False
        if fingerprint(record["signature"]) != job["reference_id"]:
            return False
        expected = reference_record(job, record["evidence"], record.get("provenance"))
        if not expected["qualified"] or any(record.get(k) != expected[k] for k in
                (*PHYSICAL_FIELDS, "schema_version", "physical_id", "status", "qualified", "source_sha", "method", "software", "controls",
                 "qualification", "uncertainty_db", "ts_db", "amplitude_real_m", "amplitude_imag_m")):
            return False
        return True
    except (ValueError, TypeError, KeyError, AttributeError):
        return False


def cached_reference(path, job):
    """Reject stale, malformed or unqualified cache entries."""
    try:
        record = read_json(path)
        return record if valid_cached(record, job) else None
    except (OSError, ValueError):
        return None


def export_references(jobs, cache_dir, output):
    records = [record for job in jobs
               if (record := cached_reference(Path(cache_dir) / (job["reference_id"] + ".json"), job))]
    summary = dict(schema_version=1, updated_at=utc_now(), planned=len(jobs),
                   qualified=len(records), references=records)
    atomic_json(output, summary)
    return summary


def _stop(process, job):
    if job is not None:
        job.terminate()
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.wait(timeout=10)


def _run_batch(jobs, runtime, work, threads, deadline, case_timeout, accept, progress, *, attempts_dir=None):
    """Keep one Julia process warm while enforcing a deadline for each reference."""
    attempts_dir = Path(attempts_dir) if attempts_dir is not None else work / "reference-attempts"
    request = work / "reference-batch.json"
    marker = work / "reference-active.json"
    marker.unlink(missing_ok=True)
    for item in jobs:
        (attempts_dir / (item["reference_id"] + ".json")).unlink(missing_ok=True)
    started = time.time()
    atomic_json(request, dict(cases=jobs, threads=threads, deadline_epoch=deadline, started_epoch=started,
                             case_timeout_seconds=case_timeout, work_directory=str(work),
                             active_path=str(marker), attempts_directory=str(attempts_dir)))
    prefix = list(runtime["executables"]["acousticscattering"])
    # Prepared runtime prefixes contain --threads=1 for timing workers.
    prefix = [part for part in prefix if not part.startswith("--threads=")]
    command = [*prefix, f"--threads={threads}", str(ROOT / "src/worker_reference.jl"), str(request)]
    env = dict(os.environ, **runtime.get("env", {}))
    env.update(JULIA_NUM_THREADS=str(threads), OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1",
               MKL_NUM_THREADS="1", VECLIB_MAXIMUM_THREADS="1")
    gate = work / "reference-launch.ready"
    gate.unlink(missing_ok=True)
    owner = None
    if os.name == "nt":
        from process_control import WindowsJob
        owner = WindowsJob()
        command = [sys.executable, str(ROOT / "src/process_gate.py"), str(gate), *command]
    process = None
    stopped = False
    collected = set()
    active = dict(reference_id=jobs[0]["reference_id"], started_epoch=started, phase="importing")
    next_result = 0

    def collect():
        nonlocal next_result
        while next_result < len(jobs):
            item = jobs[next_result]
            identity = item["reference_id"]
            path = attempts_dir / (identity + ".json")
            if not path.exists():
                break
            accept(item, read_json(path))
            collected.add(identity)
            next_result += 1

    try:
        with (work / "reference-worker.log").open("a", encoding="utf-8") as log:
            process = subprocess.Popen(command, cwd=work, env=env, stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=os.name != "nt",
                                       creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            if owner is not None:
                owner.assign(process)
                gate.touch()
            while True:
                collect()
                if marker.exists():
                    updated = read_json(marker)
                    if updated != active:
                        active = updated
                        progress(active)
                if process.poll() is not None:
                    collect()  # The final result can be committed just before process exit.
                    break
                expired = time.time() >= deadline
                if expired or time.time() - active["started_epoch"] >= case_timeout:
                    _stop(process, owner)
                    stopped = True
                    identity = active.get("reference_id")
                    if identity not in collected:
                        item = next((j for j in jobs if j["reference_id"] == identity), jobs[0])
                        evidence = dict(status="timeout", message="Reference deadline reached",
                                        phase=active.get("phase"), elapsed_seconds=time.time() - active["started_epoch"])
                        atomic_json(attempts_dir / (item["reference_id"] + ".json"), evidence)
                        accept(item, evidence)
                        collected.add(item["reference_id"])
                    break
                time.sleep(0.2)
        if process.returncode and not stopped:
            identity = active.get("reference_id")
            if identity not in collected:
                item = next((j for j in jobs if j["reference_id"] == identity), jobs[0])
                evidence = dict(status="worker_error", message=f"Julia reference worker exited {process.returncode}",
                                phase=active.get("phase"))
                atomic_json(attempts_dir / (item["reference_id"] + ".json"), evidence)
                accept(item, evidence)
                collected.add(item["reference_id"])
    finally:
        if process is not None and not stopped:
            _stop(process, owner)
        if owner is not None:
            owner.close()
        gate.unlink(missing_ok=True)
    return collected


def run(args):
    configuration = read_json(args.config)
    lock = read_json(args.sources_lock)
    sha = lock["sources"]["acousticscattering"]["sha"]
    jobs = reference_jobs(configuration, args.profile, sha, args.threads)
    if not 0 <= args.shard_index < args.shard_count:
        raise ValueError("shard_index must be in [0, shard_count)")
    # Stable across profile expansion or reordering.
    jobs = [job for job in jobs if int(job["physical_id"], 16) % args.shard_count == args.shard_index]
    output = Path(args.output).resolve()
    work = output.parent
    work.mkdir(parents=True, exist_ok=True)
    cache = Path(args.cache_dir).resolve()
    if args.plan:
        atomic_json(output, dict(schema_version=1, planned=len(jobs), requests=jobs))
        return 0
    existing = export_references(jobs, cache, output)["references"]
    status_path = work / "reference-status.json"
    attempted = {}
    accepted = {record["reference_id"]: record for record in existing}

    def status(state, active=None):
        records = [accepted[job["reference_id"]] for job in jobs if job["reference_id"] in accepted]
        if active is None:
            atomic_json(output, dict(schema_version=1, updated_at=utc_now(), planned=len(jobs),
                                     qualified=len(records), references=records))
        atomic_json(status_path, dict(state=state, updated_at=utc_now(), planned=len(jobs),
                    qualified=len(records), cached_at_start=len(existing), attempted=len(attempted),
                    failures=sum(not item["qualified"] for item in attempted.values()), current=active))
        return records

    if args.export_only:
        status("exported")
        return 0
    if len(existing) == len(jobs):
        status("completed")
        print(f"Qualified references: {len(existing)}/{len(jobs)}, all cached", flush=True)
        return 0
    if not args.runtime:
        raise ValueError("--runtime is required for reference calculation")
    runtime = read_json(args.runtime)
    if runtime.get("source_shas", {}).get("acousticscattering") != sha:
        raise ValueError("Reference runtime source does not match the lock")
    if runtime.get("availability", {}).get("acousticscattering", {}).get("status") != "ready":
        raise ValueError("AcousticScattering reference runtime is not ready")
    positive(args.budget_seconds, "budget_seconds")
    positive(args.case_timeout_seconds, "case_timeout_seconds")
    if args.case_limit is not None and args.case_limit < 1:
        raise ValueError("case_limit must be positive")
    found = {record["reference_id"] for record in existing}
    todo = [job for job in jobs if job["reference_id"] not in found]
    if args.case_limit:
        todo = todo[:args.case_limit]
    deadline = min(time.time() + args.budget_seconds, float(os.getenv("BENCHMARK_DEADLINE", "inf")))
    provenance = dict(environment_fingerprint=runtime.get("environment_fingerprint"),
                      environment_compatibility=julia_provenance(runtime),
                      source_sha=sha, threads=args.threads, blas_threads=1,
                      github_run_id=os.getenv("GITHUB_RUN_ID"), github_run_attempt=os.getenv("GITHUB_RUN_ATTEMPT"))

    def accept(job, evidence):
        record = reference_record(job, evidence, provenance)
        attempted[job["reference_id"]] = record
        if record["qualified"]:
            atomic_json(cache / (job["reference_id"] + ".json"), record)
            accepted[job["reference_id"]] = record
        # Keep failures and complete qualification evidence outside the reusable cache.
        atomic_json(work / "reference-attempts" / (job["reference_id"] + ".record.json"), record)
        status("running")
        print(f"REFERENCE {job['reference_id']} {record['status']} qualified={record['qualified']}", flush=True)

    status("running")
    while todo and deadline - time.time() >= args.case_timeout_seconds:
        completed = _run_batch(todo, runtime, work, args.threads, deadline, args.case_timeout_seconds,
                               accept, lambda active: status(active["phase"], active))
        if not completed:
            break
        todo = [job for job in todo if job["reference_id"] not in completed]
    records = status("completed" if len(existing) + len(attempted) == len(jobs) else "budget_paused")
    print(f"Qualified references: {len(records)}/{len(jobs)}", flush=True)
    return 1 if any(not record["qualified"] for record in attempted.values()) else 0


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--config", default=str(ROOT / "benchmark.json"))
    result.add_argument("--sources-lock", default=str(ROOT / "sources.lock.json"))
    result.add_argument("--profile", default="pilot")
    result.add_argument("--runtime")
    result.add_argument("--cache-dir", required=True)
    result.add_argument("--output", required=True)
    result.add_argument("--threads", type=int, default=4)
    result.add_argument("--budget-seconds", type=float, default=1800)
    result.add_argument("--case-timeout-seconds", type=float, default=600)
    result.add_argument("--shard-index", type=int, default=0)
    result.add_argument("--shard-count", type=int, default=1)
    result.add_argument("--case-limit", type=int)
    result.add_argument("--plan", action="store_true")
    result.add_argument("--export-only", action="store_true")
    return result


if __name__ == "__main__":
    raise SystemExit(run(parser().parse_args()))
