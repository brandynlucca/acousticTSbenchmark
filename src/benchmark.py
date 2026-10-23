"""Sequential, checkpointed timing of matched spheroid backscatter problems."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone

from process_control import PredictionMemory, memory_provenance

ROOT = Path(__file__).resolve().parents[1]
BOUNDARIES = ("rigid", "pressure_release", "liquid", "gas")
CAPABILITIES = {
    "acousticts": BOUNDARIES, "acousticscattering": BOUNDARIES,
    "echosms": BOUNDARIES, "liquid_spheroid": BOUNDARIES,
    "prol_spheroid": ("liquid", "gas"),
}
STATUSES = {"ok", "nonconverged", "numerical_error", "unsupported", "setup_error",
            "timeout", "setup_timeout", "setup_unavailable", "worker_error"}
PHYSICAL_FIELDS = ("boundary", "frequency_hz", "incidence_deg", "semimajor_m",
                   "semiminor_m", "medium_density_kg_m3", "medium_sound_speed_m_s",
                   "target_density_kg_m3", "target_sound_speed_m_s")


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False,
                                     separators=(",", ":")).encode()).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def physical_identity(problem):
    return fingerprint({key: (float(problem[key]) if problem[key] is not None and key != "boundary"
                              else problem[key]) for key in PHYSICAL_FIELDS})


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def positive(value, name):
    if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and positive")


def frequencies(configuration):
    specification = configuration["frequency"]
    start, stop, step = (specification[k] for k in ("start_hz", "stop_hz", "step_hz"))
    for key, value in (("start_hz", start), ("stop_hz", stop), ("step_hz", step)):
        positive(value, key)
        if int(value) != value:
            raise ValueError(f"{key} must be an integer number of Hz")
    if start > stop:
        raise ValueError("start_hz exceeds stop_hz")
    values = list(range(int(start), int(stop) + 1, int(step)))
    if specification.get("include_stop", True) and values[-1] != stop:
        values.append(int(stop))
    return values


def build_cases(configuration, profile="pilot", software=None, repetitions=None):
    selection = configuration["profiles"][profile]
    ratios = selection.get("aspect_ratios", configuration["geometry"]["aspect_ratios"])
    angles = selection.get("incidences_deg", configuration["incidences_deg"])
    boundaries = selection.get("boundaries", configuration["boundaries"])
    programs = software or configuration["software"]
    repeats = configuration["repetitions"] if repetitions is None else repetitions
    if not isinstance(repeats, int) or repeats < 1:
        raise ValueError("repetitions must be a positive integer")
    if len(programs) != len(set(programs)) or not programs:
        raise ValueError("software selection must be nonempty and unique")
    for name in programs:
        if name not in CAPABILITIES:
            raise ValueError(f"Unknown software: {name}")
    medium = configuration["medium"]
    for key, value in medium.items():
        positive(value, key)
    a = configuration["geometry"]["semimajor_m"]
    positive(a, "semimajor_m")
    cases = []
    for boundary in boundaries:
        if boundary not in BOUNDARIES:
            raise ValueError(f"Unknown boundary: {boundary}")
        target = configuration["materials"].get(boundary, {})
        if boundary in ("gas", "liquid"):
            for key in ("density_kg_m3", "sound_speed_m_s"):
                positive(target[key], f"{boundary}.{key}")
        for ratio in ratios:
            positive(ratio, "aspect_ratio")
            if ratio <= 1:
                raise ValueError("Prolate aspect ratio must exceed one")
            for angle in angles:
                if not isinstance(angle, (int, float)) or not math.isfinite(angle) or not 0 <= angle <= 90:
                    raise ValueError("Incidence must be between 0 and 90 degrees from the major axis")
                for frequency_index, frequency in enumerate(frequencies(configuration)):
                    physical = dict(boundary=boundary, frequency_hz=frequency,
                                    incidence_deg=angle, semimajor_m=a, semiminor_m=a / ratio,
                                    medium_density_kg_m3=medium["density_kg_m3"],
                                    medium_sound_speed_m_s=medium["sound_speed_m_s"],
                                    target_density_kg_m3=target.get("density_kg_m3"),
                                    target_sound_speed_m_s=target.get("sound_speed_m_s"))
                    q = math.sqrt((a - a / ratio) * (a + a / ratio))
                    k0 = 2 * math.pi * frequency / medium["sound_speed_m_s"]
                    for repeat in range(repeats):
                        # Rotate software order while preserving ascending, dense spectra.
                        offset = (frequency_index + repeat) % len(programs)
                        for program in programs[offset:] + programs[:offset]:
                            case = dict(physical, software=program, repetition=repeat + 1,
                                        aspect_ratio=ratio, k0a=k0 * a, h0=k0 * q,
                                        hs=(2 * math.pi * frequency * q / target["sound_speed_m_s"]
                                            if target else None),
                                        controls=configuration["controls"][program],
                                        warmup=configuration["warmup"])
                            case["physical_id"] = physical_identity(physical)
                            case["case_id"] = fingerprint(case)
                            cases.append(case)
    if len({c["case_id"] for c in cases}) != len(cases):
        raise ValueError("Duplicate cases in configuration")
    return cases


def environment():
    processor = platform.processor() or os.environ.get("PROCESSOR_IDENTIFIER", "")
    if not processor and platform.system() == "Linux":
        try:
            processor = next(line.split(":", 1)[1].strip() for line in
                             Path("/proc/cpuinfo").read_text().splitlines()
                             if line.startswith("model name"))
        except (OSError, StopIteration):
            pass
    return dict(platform=platform.system(), architecture=platform.machine(),
                processor=processor, python=sys.version,
                cpu_count=os.cpu_count(), image_os=os.getenv("ImageOS"),
                image_version=os.getenv("ImageVersion"), runner_name=os.getenv("RUNNER_NAME"),
                github_run_id=os.getenv("GITHUB_RUN_ID"),
                github_run_attempt=os.getenv("GITHUB_RUN_ATTEMPT"),
                threads=1, memory_measurement=memory_provenance())


def adapter_hashes():
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted((ROOT / "src").glob("*")) if p.is_file()}


def stop_process(process, job=None):
    if job is not None:
        job.terminate()
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.wait(timeout=10)


def command_for(case, runtime, request, response):
    name = case["software"]
    prefix = runtime.get("executables", {}).get(name)
    if not prefix or not isinstance(prefix, list):
        raise ValueError(f"No executable configured for {name}")
    worker = ("worker_acousticts.R" if name == "acousticts" else
              "worker_julia.jl" if name in ("acousticscattering", "liquid_spheroid")
              else "worker_python.py")
    return [*prefix, str(ROOT / "src" / worker), str(request), str(response)]


def measure(case, runtime, directory, timeout_seconds):
    name = case["software"]
    if case["boundary"] not in CAPABILITIES[name]:
        return dict(status="unsupported", elapsed_seconds=None, message="Boundary not supported")
    availability = runtime.get("availability", {}).get(name)
    if availability and availability["status"] != "ready":
        return dict(status=availability["status"], elapsed_seconds=None,
                    message=availability.get("message", "Unavailable runtime"))
    quad = runtime.get("capabilities", {}).get(name, {}).get("quad", {})
    if name == "acousticts" and case["controls"].get("precision") == "quad" and quad.get("available") is False:
        return dict(status="unsupported", elapsed_seconds=None, message=quad["message"])
    directory.mkdir(parents=True, exist_ok=True)
    request, response = directory / "request.json", directory / "response.json"
    marker = directory / "timing_started.json"
    for path in (response, marker):
        path.unlink(missing_ok=True)
    payload = dict(case, source_path=runtime.get("source_paths", {}).get(name),
                   work_directory=str(directory), timing_started_path=str(marker),
                   native_executable=runtime.get("native_executables", {}).get(name))
    memory = PredictionMemory(directory)
    payload.update(memory.request_fields())
    atomic_json(request, payload)
    try:
        command = command_for(case, runtime, request, response)
    except ValueError as error:
        return dict(status="setup_error", elapsed_seconds=None, message=str(error))
    env = os.environ.copy()
    env.update(runtime.get("env", {}))
    # Publishing credentials belong only to this supervisor, never to solvers.
    for key in ("GITHUB_TOKEN", "GH_TOKEN"):
        env.pop(key, None)
    env.update(OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1", MKL_NUM_THREADS="1",
               JULIA_NUM_THREADS="1", VECLIB_MAXIMUM_THREADS="1", NUMEXPR_NUM_THREADS="1",
               MPLBACKEND="Agg", PYTHONDONTWRITEBYTECODE="1")
    started = time.monotonic()
    job = None
    gate = directory / "launch.ready"
    gate.unlink(missing_ok=True)
    if os.name == "nt":
        from process_control import WindowsJob
        job = WindowsJob()
        command = [sys.executable, str(ROOT / "src/process_gate.py"), str(gate), *command]
    with (directory / "worker.log").open("w", encoding="utf-8") as log:
        try:
            process = subprocess.Popen(command, cwd=directory, env=env, stdout=log,
                                       stderr=subprocess.STDOUT, start_new_session=os.name != "nt",
                                       creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        except OSError as error:
            if job:
                job.close()
            return dict(status="setup_error", elapsed_seconds=None, message=str(error))
        if job:
            try:
                job.assign(process)
                gate.touch()
            except OSError:
                process.kill()
                process.wait(timeout=10)
                job.close()
                raise
        memory.start(process.pid)
        memory_result = None
        try:
            process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            memory_result = memory.finish(censored=True)
            stop_process(process, job)
            timing = read_json(marker) if marker.exists() else {}
            return dict(status="timeout" if timing else "setup_timeout", elapsed_seconds=None,
                        wall_seconds=time.monotonic() - started,
                        solve_lower_bound_seconds=max(0, time.time() - timing["started_epoch"])
                        if timing else None, message=f"Worker exceeded {timeout_seconds:g} seconds", **memory_result)
        except BaseException:
            memory_result = memory.finish(censored=True)
            stop_process(process, job)
            raise
        finally:
            if memory_result is None:
                memory_result = memory.finish()
            if job:
                job.close()
    wall_seconds = time.monotonic() - started
    if process.returncode or not response.exists():
        return dict(status="worker_error", elapsed_seconds=None, wall_seconds=wall_seconds,
                    message=f"Worker exit {process.returncode}, see worker.log", **memory_result)
    try:
        result = read_json(response)
        if result.get("status") not in STATUSES:
            raise ValueError("Invalid worker status")
        if result["status"] == "ok":
            positive(result["elapsed_seconds"], "elapsed_seconds")
            if not isinstance(result.get("ts_db"), (int, float)) or not math.isfinite(result["ts_db"]):
                raise ValueError("Worker returned nonfinite TS")
        json.dumps(result, allow_nan=False)
    except (ValueError, KeyError, TypeError) as error:
        return dict(status="worker_error", elapsed_seconds=None, wall_seconds=wall_seconds,
                    message=f"Invalid worker result: {error}", **memory_result)
    return dict(result, wall_seconds=wall_seconds, **memory_result)


def load_references(path):
    if path is None:
        return {}
    result = {}
    for record in read_json(path)["references"]:
        physical = {key: record[key] for key in PHYSICAL_FIELDS}
        identity = physical_identity(physical)
        if identity in result:
            raise ValueError("Duplicate reference physical case")
        if record.get("qualified") is not True:
            continue
        if (not record.get("reference_id") or not record.get("source_sha") or
                not record.get("qualification") or not record.get("software") or
                not isinstance(record.get("controls"), dict)):
            raise ValueError("Qualified references require identity, software, source SHA, controls and qualification evidence")
        if not math.isfinite(record["ts_db"]):
            raise ValueError("Nonfinite reference TS")
        positive(record["uncertainty_db"], "reference uncertainty_db")
        result[identity] = record
    return result


def summarize(cases, records, references):
    rows = []
    for case in cases:
        record = records.get(case["case_id"], {})
        row = dict(case, **{k: v for k, v in record.items() if k not in case})
        row.setdefault("status", "pending")
        row["delta_ts_db"] = None
        reference = references.get(case["physical_id"])
        if reference:
            row["reference_id"] = reference["reference_id"]
            row["reference_source_sha"] = reference["source_sha"]
            row["reference_ts_db"] = reference["ts_db"]
            row["reference_qualification"] = reference["qualification"]
            row["reference_fingerprint"] = fingerprint(reference)
            row["reference_uncertainty_db"] = reference["uncertainty_db"]
            same = (row.get("software") == reference.get("software") and
                    row.get("source_sha") == reference.get("source_sha") and
                    row.get("controls") == reference.get("controls"))
            if row["status"] == "ok" and not same:
                row["delta_ts_db"] = row["ts_db"] - reference["ts_db"]
        rows.append(row)
    return rows


def export(directory, cases, records, references, state):
    from report import render_report

    rows = summarize(cases, records, references)
    atomic_json(directory / "results.json", rows)
    fields = list(dict.fromkeys(key for row in rows for key in row))
    temporary = directory / "results.csv.tmp"
    with temporary.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v, sort_keys=True) if isinstance(v, (dict, list)) else v
                             for k, v in row.items()})
    os.replace(temporary, directory / "results.csv")
    counts = {status: sum(row["status"] == status for row in rows)
              for status in sorted({row["status"] for row in rows})}
    status = dict(state=state, updated_at=utc_now(), planned=len(cases),
                  recorded=len(records), outcomes=counts)
    atomic_json(directory / "status.json", status)
    render_report(directory / "progress.html", rows, status)
    return status


class ProgressPublisher:
    """Publish synchronously between workers, with bounded requests and backoff."""

    def __init__(self, enabled=False):
        self.enabled = enabled
        self.next_update = float("-inf")
        self.retry_after = float("-inf")
        required = ("GITHUB_TOKEN", "GITHUB_REPOSITORY", "BENCHMARK_PLATFORM", "GITHUB_RUN_ID")
        if enabled and not all(os.environ.get(key) for key in required):
            print("Progress publication disabled: GitHub run settings are incomplete", file=sys.stderr)
            self.enabled = False

    def due(self, *, first_result=False, force=False):
        now = time.monotonic()
        return self.enabled and (force or (now >= self.retry_after and
                                           (first_result or now >= self.next_update)))

    def publish(self, directory, status, *, first_result=False, force=False):
        if not self.due(first_result=first_result, force=force):
            return
        try:
            from publish_progress import GitHub, compact_payload, publish

            # A new API instance gives each publication its own short deadline.
            repository = os.environ["GITHUB_REPOSITORY"]
            api = GitHub(repository, os.environ["GITHUB_TOKEN"], timeout_seconds=15)
            payload = compact_payload(repository, os.environ["BENCHMARK_PLATFORM"],
                int(os.environ["GITHUB_RUN_ID"]), int(os.getenv("GITHUB_RUN_ATTEMPT", "1")),
                read_json(directory / "results.json"), status, "running")
            publish(api, payload)
            self.retry_after = float("-inf")
        except Exception as error:
            # Error messages can contain URLs or credentials supplied by libraries.
            # Report only the exception type and keep every recorded measurement.
            print(f"Progress publication failed ({type(error).__name__}), retrying after 30 seconds",
                  file=sys.stderr, flush=True)
            self.retry_after = time.monotonic() + 30
        self.next_update = time.monotonic() + 30


def run(args):
    positive(args.budget_seconds, "budget_seconds")
    positive(args.chunk_seconds, "chunk_seconds")
    configuration = read_json(args.config)
    lock = read_json(args.sources_lock)
    cases = build_cases(configuration, args.profile,
                        args.software.split(",") if args.software else None, args.repetitions)
    if any(case["software"] == "acousticscattering" for case in cases):
        raise ValueError("AcousticScattering is reference-only. Run the Cached numerical references workflow")
    if args.plan:
        print(json.dumps(dict(cases=len(cases), frequencies=frequencies(configuration),
                              software=sorted({c["software"] for c in cases}),
                              profile=args.profile, first_case=cases[0]), indent=2))
        return 0
    if not args.runtime:
        raise ValueError("--runtime is required for execution")
    runtime = read_json(args.runtime)
    if not runtime.get("environment_fingerprint"):
        raise ValueError("Runtime requires an environment_fingerprint")
    references = load_references(args.references)
    for software in {c["software"] for c in cases}:
        if runtime.get("source_shas", {}).get(software) != lock["sources"][software]["sha"]:
            raise ValueError(f"Runtime source does not match lock for {software}")
    directory = Path(args.output).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    identity = dict(configuration=configuration, sources=lock, profile=args.profile,
                    cases=[c["case_id"] for c in cases], adapter_hashes=adapter_hashes(),
                    platform=platform.system(), architecture=platform.machine(),
                    environment_fingerprint=fingerprint(dict(
                        runtime=runtime["environment_fingerprint"], memory_observer=memory_provenance())),
                    workload="fresh_process_warmed_distinct_problem_single_prediction")
    from references import build_requests
    manifest = dict(identity, fingerprint=fingerprint(identity),
                    reference_requests=build_requests(configuration, lock, args.profile))
    manifest_path = directory / "manifest.json"
    if manifest_path.exists():
        if read_json(manifest_path)["fingerprint"] != manifest["fingerprint"]:
            raise ValueError("Resume refused: cases, sources, adapters or environment changed")
    else:
        atomic_json(manifest_path, manifest)
    # Exclusive creation prevents concurrent timing processes sharing this campaign.
    mutex = directory / "running.lock"
    try:
        descriptor = os.open(mutex, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as error:
        raise ValueError("Campaign lock exists. Verify no worker is active before removing running.lock") from error
    os.write(descriptor, f"{os.getpid()}\n".encode())
    os.close(descriptor)
    try:
        invocation = f"{time.time_ns()}-{os.getpid()}"
        atomic_json(directory / "environments" / f"{invocation}.json",
                    dict(environment(), runtime=runtime, started_at=utc_now(),
                         references_fingerprint=fingerprint(references)))
        checkpoint_directory = directory / "records"
        checkpoint_directory.mkdir(exist_ok=True)
        records = {p.stem: read_json(p) for p in checkpoint_directory.glob("*.json")}
        expected = {c["case_id"] for c in cases}
        if records.keys() - expected:
            raise ValueError("Checkpoint contains cases outside manifest")
        status = export(directory, cases, records, references, "running")
        publisher = ProgressPublisher(args.publish_progress)
        publisher.publish(directory, status, force=True)
        last_export = time.monotonic()
        blocked = {}
        def setup_group(case):
            return (case["software"], case["boundary"], case["incidence_deg"])
        for case in cases:
            previous = records.get(case["case_id"], {})
            if previous.get("status") in ("setup_error", "setup_timeout"):
                blocked[setup_group(case)] = case["case_id"]
        started = time.monotonic()
        absolute_deadline = float(os.environ.get("BENCHMARK_DEADLINE", "inf"))
        deadline = min(started + args.budget_seconds,
                       started + max(0, absolute_deadline - time.time()))
        timeout = configuration["case_timeout_seconds"]
        positive(timeout, "case_timeout_seconds")
        completed = 0
        state = "complete"
        for case in cases:
            if case["case_id"] in records:
                continue
            if (time.monotonic() - started >= args.chunk_seconds or
                    deadline - time.monotonic() < timeout or
                    (args.max_cases is not None and completed >= args.max_cases)):
                state = "budget_paused"
                break
            print(f"START {len(records) + 1}/{len(cases)} {case['software']} "
                  f"{case['boundary']} AR={case['aspect_ratio']} angle={case['incidence_deg']} "
                  f"f={case['frequency_hz']} Hz", flush=True)
            group = setup_group(case)
            if group in blocked:
                result = dict(status="setup_unavailable", elapsed_seconds=None,
                              message="Not attempted after setup failure", setup_failure_case=blocked[group])
            else:
                result = measure(case, runtime, directory / "work" / case["case_id"], timeout)
                if result["status"] in ("setup_error", "setup_timeout"):
                    blocked[group] = case["case_id"]
            record = dict(result, case_id=case["case_id"], invocation_id=invocation,
                          source_sha=lock["sources"][case["software"]]["sha"],
                          finished_at=utc_now())
            atomic_json(checkpoint_directory / f"{case['case_id']}.json", record)
            records[case["case_id"]] = record
            completed += 1
            publish_now = publisher.due(first_result=completed == 1)
            if publish_now or time.monotonic() - last_export >= 30:
                status = export(directory, cases, records, references, "running")
                last_export = time.monotonic()
            print(f"DONE {len(records)}/{len(cases)} {result['status']} "
                  f"solve_seconds={result.get('elapsed_seconds')}", flush=True)
            if publish_now:
                publisher.publish(directory, status, first_result=completed == 1)
        status = export(directory, cases, records, references, state)
        publisher.publish(directory, status, force=True)
        print(json.dumps(status), flush=True)
    finally:
        mutex.unlink(missing_ok=True)
    return 0


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--config", default=str(ROOT / "benchmark.json"))
    result.add_argument("--sources-lock", default=str(ROOT / "sources.lock.json"))
    result.add_argument("--profile", default="pilot")
    result.add_argument("--software", help="Comma-separated software identifiers")
    result.add_argument("--repetitions", type=int)
    result.add_argument("--plan", action="store_true")
    result.add_argument("--runtime")
    result.add_argument("--references", default=os.environ.get("BENCHMARK_REFERENCES") or None)
    result.add_argument("--output", default=str(ROOT / ".scratch/codex/dense-benchmark/run"))
    result.add_argument("--budget-seconds", type=float, default=2700)
    result.add_argument("--chunk-seconds", type=float, default=300)
    result.add_argument("--max-cases", type=int)
    result.add_argument("--publish-progress", action="store_true",
                        help="Publish running snapshots between workers using GitHub workflow settings")
    return result


if __name__ == "__main__":
    try:
        raise SystemExit(run(parser().parse_args()))
    except (ValueError, KeyError) as error:
        print(f"Configuration error: {error}", file=sys.stderr)
        raise SystemExit(2)
