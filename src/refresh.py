"""Restart selected software while preserving completed comparison measurements."""

from __future__ import annotations

import copy
import hashlib
import io
import os
from pathlib import PurePosixPath
import re
import zipfile

from benchmark import atomic_json, build_cases, fingerprint, read_json, ROOT, STATUSES, summarize
from publish_progress import PLATFORMS, json_size_limit, retained_comparison_rows, write_retained


def _measurement_adapters(program):
    worker = ("worker_acousticts.R" if program == "acousticts" else
              "worker_julia.jl" if program == "liquid_spheroid" else "worker_python.py")
    names = (worker, "process_control.py", "process_gate.py")
    if worker == "worker_python.py":
        names += ("python-constraints.txt",)
    result = {}
    for name in names:
        raw = (ROOT / "src" / name).read_bytes()
        normalized = raw.replace(b"\r\n", b"\n")
        result[name] = {hashlib.sha256(value).hexdigest() for value in
                        (raw, normalized, normalized.replace(b"\n", b"\r\n"))}
    return result


def _runtime_selection(runtime, program):
    """Compare relevant build inputs without unrelated installed software."""
    evidence = runtime.get("environment_compatibility", {})
    result = dict(source_sha=runtime.get("source_shas", {}).get(program))
    if program in ("prol_spheroid", "echosms"):
        packages = {}
        for line in evidence.get("python_packages", "").splitlines():
            name, delimiter, version = line.partition("==")
            if delimiter and name.lower() in ("numpy", "scipy", "pandas", "matplotlib"):
                packages[name.lower()] = version
        if len(packages) != 4:
            raise ValueError("Reused Python measurements lack dependency version evidence")
        result["python_packages"] = packages
        if program == "prol_spheroid":
            for key in ("prol_compiler", "prol_flags"):
                if not evidence.get(key):
                    raise ValueError("Reused Prol_Spheroid measurements lack compiler evidence")
                result[key] = evidence[key]
    elif program == "acousticts":
        if not evidence.get("r_packages"):
            raise ValueError("Reused acousticTS measurements lack R dependency evidence")
        result["r_packages"] = evidence["r_packages"]
    else:
        julia = evidence.get("julia_environments", {}).get(program)
        if not julia:
            raise ValueError("Reused measurements lack Julia dependency evidence")
        result["julia_environment"] = julia
        result.update({key: value for key, value in evidence.items() if key.startswith("liquid_")})
    return result


def validate_reused_records(sidecar, cases, sources, platform=None, runtime=None):
    """Validate promoted observations against their original measurement evidence."""
    from campaign import manifest_identity
    records = sidecar.get("reused_records", {})
    if not isinstance(records, dict):
        raise ValueError("Invalid reused measurement metadata")
    by_id = {case["case_id"]: case for case in cases}
    proofs = sidecar.get("provenance", [])
    if not isinstance(proofs, list):
        raise ValueError("Reused measurements require provenance")
    verified, checked_manifests, checked_environments, adapters = {}, set(), set(), {}
    proof_cases = [(proof, set(proof.get("manifest", {}).get("cases", [])))
                   for proof in proofs if isinstance(proof, dict)]
    for identity, record in records.items():
        case = by_id.get(identity)
        if (case is None or not isinstance(record, dict) or record.get("case_id") != identity or
                any(record.get(key) != value for key, value in case.items()) or
                record.get("status") not in STATUSES or record.get("measurement_role") == "retained" or
                type(record.get("measurement_origin_run_id")) is not int or
                record["measurement_origin_run_id"] < 1):
            raise ValueError("Reused measurements do not match the selected prediction cases")
        program = case["software"]
        expected_source = sources.get("sources", {}).get(program, {}).get("sha")
        if (not isinstance(expected_source, str) or not re.fullmatch(r"[a-f0-9]{40}", expected_source) or
                record.get("source_sha") != expected_source):
            raise ValueError("Reused measurement source differs from the selected software revision")
        candidates = [proof for proof, identities in proof_cases if
                      proof.get("run_id") == record["measurement_origin_run_id"] and
                      identity in identities]
        if len(candidates) != 1:
            raise ValueError("Reused measurement requires exactly one original manifest")
        proof = candidates[0]
        manifest = proof["manifest"]
        proof_key = (id(proof), program)
        if proof_key not in checked_manifests:
            if program not in adapters:
                adapters[program] = _measurement_adapters(program)
            if (fingerprint(manifest_identity(manifest)) != manifest.get("fingerprint") or
                    manifest.get("sources", {}).get("sources", {}).get(program, {}).get("sha") != expected_source or
                    manifest.get("workload") != "fresh_process_warmed_distinct_problem_single_prediction" or
                    any(manifest.get("adapter_hashes", {}).get(name) not in values
                        for name, values in adapters[program].items()) or
                    (platform is not None and manifest.get("platform") != platform)):
                raise ValueError("Reused measurement adapters, workload, platform, or source are incompatible")
            checked_manifests.add(proof_key)
        environments = proof.get("environments", {})
        invocation = environments.get("invocations", {}).get(record.get("invocation_id"), {})
        environment = environments.get("evidence", {}).get(invocation.get("environment_fingerprint"))
        attempted = record["status"] not in {"setup_error", "setup_timeout", "setup_unavailable", "unsupported"}
        environment_key = (*proof_key, invocation.get("environment_fingerprint"), attempted)
        if environment_key not in checked_environments:
            if (not isinstance(environment, dict) or fingerprint(environment) != invocation.get("environment_fingerprint") or
                    environment.get("platform") != manifest.get("platform") or
                    environment.get("architecture") != manifest.get("architecture") or
                    environment.get("threads") != 1):
                raise ValueError("Reused measurement lacks matching original environment evidence")
            original_runtime = environment.get("runtime", {})
            if original_runtime.get("source_shas", {}).get(program) != expected_source:
                raise ValueError("Reused environment has a different software revision")
            if attempted:
                original = _runtime_selection(original_runtime, program)
                if runtime is not None and original != _runtime_selection(runtime, program):
                    raise ValueError("Reused measurement compiler or dependencies differ from the current runtime")
            checked_environments.add(environment_key)
        verified[identity] = record
    return verified


def baseline_settings(content, repository, run_id, platform):
    """Recognize an immutable refresh baseline saved before platform setup."""
    from campaign import MAX_RECORD_BYTES, MAX_SELECTED_BYTES, _json, validate_plan_size
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        names = archive.namelist()
        if "manifest.json" in names or "refresh-campaign.json" not in names:
            return None
        allowed = {"refresh-campaign.json", "retained-results.json", "benchmark-source.json"}
        seed_paths = {name for name in names if re.fullmatch(r"records/[a-f0-9]{64}\.json", name)}
        if set(names) - allowed - seed_paths:
            raise ValueError("Unexpected files in a refresh baseline")
        if len(names) != len(set(names)) or "retained-results.json" not in names:
            raise ValueError("Incomplete or duplicate refresh baseline metadata")
        values, total = {}, 0
        for name in ("refresh-campaign.json", "retained-results.json", *sorted(seed_paths)):
            entry = archive.getinfo(name)
            total += entry.file_size
            maximum = MAX_RECORD_BYTES if name in seed_paths else json_size_limit(name)
            if (entry.file_size > maximum or total > MAX_SELECTED_BYTES or
                    (entry.external_attr >> 16) & 0o170000 == 0o120000):
                raise ValueError("Invalid refresh baseline file")
            values[name] = _json(archive.read(entry))
    meta, retained = values["refresh-campaign.json"], values["retained-results.json"]
    if (meta.get("schema_version") != 1 or meta.get("repository") != repository or
            meta.get("run_id") != int(run_id) or meta.get("platform") != platform or
            meta.get("retained_fingerprint") != fingerprint(retained)):
        raise ValueError("Refresh baseline provenance does not match its run or measurements")
    settings = meta["settings"]
    from campaign import CAPABILITIES
    programs = settings["software"].split(",")
    if (not programs or len(programs) != len(set(programs)) or
            set(programs) - (CAPABILITIES.keys() - {"acousticscattering"}) or
            not re.fullmatch(r"[a-f0-9]{40}", settings["source_ref"]) or
            fingerprint(meta["sources"]) != settings["sources_fingerprint"]):
        raise ValueError("Invalid refresh baseline source settings")
    validate_plan_size(settings["configuration"], settings["profile"], programs, settings["repetitions"])
    cases = build_cases(settings["configuration"], settings["profile"], programs, settings["repetitions"])
    if settings["planned_cases"] != len(cases):
        raise ValueError("Refresh baseline case count does not match its settings")
    retained_comparison_rows(retained, cases)
    seeds = validate_reused_records(retained, cases, meta["sources"],
        platform={"ubuntu-latest": "Linux", "windows-latest": "Windows", "macos-latest": "Darwin"}[platform])
    expected_seeds = {f"records/{identity}.json": record for identity, record in seeds.items()}
    if ({name: values[name] for name in seed_paths} != expected_seeds or
            meta.get("reused_count", 0) != len(seeds)):
        raise ValueError("Reused baseline measurements do not match their preserved records")
    return dict(settings, recorded_cases=len(seeds))


def _collection(api, run_id, name):
    items = []
    query = "filter=latest&" if name == "jobs" else ""
    for page in range(1, 101):
        part = api.request("GET", f"/actions/runs/{run_id}/{name}?{query}per_page=100&page={page}")[name]
        items.extend(part)
        if len(part) < 100:
            return items
    raise ValueError("Too many workflow records to recover the refresh")


def _run(api, run_id, repository):
    run = api.request("GET", f"/actions/runs/{run_id}")
    if (run.get("repository", {}).get("full_name") != repository or
            (run.get("head_repository") or {}).get("full_name") != repository or
            run.get("event") != "workflow_dispatch" or run.get("status") != "completed" or
            run.get("path") != ".github/workflows/benchmark.yml"):
        raise ValueError("Refresh requires this repository's completed timing workflow run")
    return run


def _read_baseline(content):
    import json
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        return (json.loads(archive.read("refresh-campaign.json")),
                json.loads(archive.read("retained-results.json")))


def _environments(content):
    from campaign import MAX_JSON_BYTES, MAX_SELECTED_BYTES, _json
    result, shared, total = {}, {}, 0
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        for entry in archive.infolist():
            path = PurePosixPath(entry.filename)
            if path.parent.name != "environments" or path.suffix != ".json":
                continue
            total += entry.file_size
            if total > MAX_SELECTED_BYTES or entry.file_size > MAX_JSON_BYTES or path.stem in result:
                raise ValueError("Invalid or oversized retained environment evidence")
            value = _json(archive.read(entry))
            observation = {key: value.pop(key) for key in ("started_at", "references_fingerprint") if key in value}
            identity = fingerprint(value)
            shared.setdefault(identity, value)
            result[path.stem] = dict(observation, environment_fingerprint=identity)
    return dict(invocations=result, evidence=shared)


def resolve_refresh(inputs, repository, revision, api, sources=None):
    from campaign import (CAPABILITIES, ORDER_CHOICES, artifact_platform, checkpoint_settings,
                          continuation_frequency, continuation_platform, csv_values, read_checkpoint,
                          select_resume_artifacts, setup_only_checkpoint, timing_or_restore_started,
                          validate_plan_size)
    requested = str(inputs.get("resume_run_id", "")).strip()
    selected_platform = continuation_platform(inputs)
    mode = inputs.get("mode", "refresh")
    extend = mode == "extend"
    frequency = continuation_frequency(inputs, mode)
    if mode not in ("refresh", "extend"):
        raise ValueError("Selected software continuation must use refresh or extend mode")
    if not requested.isdecimal() or int(requested) < 1:
        raise ValueError("Refresh mode requires a positive resume_run_id")
    if not re.fullmatch(r"[a-f0-9]{40}", revision):
        raise ValueError("Refresh requires an exact new benchmark source SHA")
    programs = csv_values(inputs.get("refresh_software", "acousticts"), "refresh_software", str.lower)
    if set(programs) - (CAPABILITIES.keys() - {"acousticscattering"}):
        raise ValueError("Refresh software must contain supported timing software")
    order = inputs.get("case_order", "Complete each frequency sweep")
    if order not in ORDER_CHOICES:
        raise ValueError("Unknown refresh case order")
    found, visited, anchor, platforms = {}, set(), None, None
    current = int(requested)
    for _ in range(9):
        if current in visited:
            raise ValueError("Cycle in timing continuation ancestry")
        visited.add(current)
        run = _run(api, current, repository)
        jobs = _collection(api, current, "jobs")
        artifacts = _collection(api, current, "artifacts")
        selected = select_resume_artifacts(artifacts, platform=selected_platform)
        if platforms is None:
            platforms = ({selected_platform} if selected_platform is not None else
                {p for p in PLATFORMS if any(job.get("name") == f"benchmark ({p})" for job in jobs)} | set(selected))
        parents = set()
        for platform, artifact in selected.items():
            content = api.archive(artifact["id"])
            baseline = baseline_settings(content, repository, current, platform)
            if baseline is not None:
                meta, retained = _read_baseline(content)
                settings = baseline
                source = dict(resumed_from_run_id=meta.get("resumed_from_run_id"))
                rows = retained["rows"] + list(retained.get("reused_records", {}).values())
                provenance = retained["provenance"]
            else:
                source = setup_only_checkpoint(content, repository, current)
                if source is not None:
                    parent = source.get("resumed_from_run_id")
                    if parent is not None:
                        parents.add(parent)
                    continue
                interrupted = run.get("conclusion") in {"cancelled", "failure"}
                settings = checkpoint_settings(content, repository, run.get("head_sha", ""), platform,
                                               allow_unexported=interrupted)
                values, records = read_checkpoint(content, allow_unexported=interrupted)
                manifest = values["manifest.json"]
                source = values.get("benchmark-source.json", {})
                cases = build_cases(settings["configuration"], settings["profile"],
                                    settings["software"].split(","), settings["repetitions"])
                table = {row["case_id"]: row for row in values["results.json"]}
                rows = [table[row["case_id"]] if table[row["case_id"]]["status"] != "pending" else row
                        for row in summarize(cases, records, {}) if row["status"] != "pending"]
                prior = values.get("retained-results.json", dict(schema_version=1, rows=[], provenance=[]))
                rows += retained_comparison_rows(prior, cases)
                provenance = list(prior["provenance"]) + [dict(run_id=current, artifact=artifact,
                    archive_sha256=hashlib.sha256(content).hexdigest(), manifest=manifest,
                    source=source, environments=_environments(content))]
            parent = source.get("resumed_from_run_id")
            if parent is not None:
                parents.add(parent)
            if anchor is None:
                anchor = settings
            # Scheduling may have changed between continuations. Physical inputs
            # and original pinned software must still identify the same campaign.
            def comparable(value):
                config = copy.deepcopy(value["configuration"])
                config.pop("case_order", None)
                return dict(configuration=config, profile=value["profile"], software=value["software"],
                            repetitions=value["repetitions"], source_ref=value["source_ref"],
                            sources_fingerprint=value["sources_fingerprint"])
            if fingerprint(comparable(settings)) != fingerprint(comparable(anchor)):
                raise ValueError("Ancestor checkpoint belongs to a different physical campaign or software revision")
            if platform in found:
                # Its newer rows are already retained, but this older artifact
                # can be the only evidence linking to a queued platform's data.
                continue
            retained_rows = [dict(row, measurement_role="retained",
                                 retained_from_run_id=row.get("retained_from_run_id", current))
                             for row in rows if row["software"] not in programs]
            found[platform] = dict(schema_version=1, rows=retained_rows, provenance=provenance)
            if extend:
                seeds = {}
                for row in rows:
                    if row["software"] not in programs:
                        continue
                    record = {key: value for key, value in row.items() if not key.startswith("reference_")
                              and key not in ("delta_ts_db", "measurement_role", "retained_from_run_id")}
                    record["measurement_origin_run_id"] = row.get("measurement_origin_run_id",
                        row.get("retained_from_run_id", current))
                    if record["case_id"] in seeds:
                        raise ValueError("Duplicate selected measurements cannot be extended")
                    seeds[record["case_id"]] = record
                found[platform]["reused_records"] = seeds
        missing = platforms - found.keys()
        if not missing:
            break
        for platform in missing:
            job = next((j for j in jobs if j.get("name") == f"benchmark ({platform})"), None)
            if ((job is not None and timing_or_restore_started(job)) or
                    any(artifact_platform(item.get("name", "")) == platform and item.get("expired") for item in artifacts)):
                raise ValueError(f"Cannot refresh {platform}: its saved measurements are unavailable")
        if len(parents) > 1:
            raise ValueError("Conflicting continuation ancestry")
        if not parents:
            break
        current = parents.pop()
        if type(current) is not int or current <= 0:
            raise ValueError("Invalid continuation parent run")
    else:
        raise ValueError("Refresh ancestry exceeds eight parent runs")
    if anchor is None:
        raise ValueError("No saved timing configuration is available to refresh")
    configuration = copy.deepcopy(anchor["configuration"])
    configuration.update(software=programs, repetitions=anchor["repetitions"], case_order=ORDER_CHOICES[order])
    if frequency is not None:
        configuration["frequency"] = frequency
    validate_plan_size(configuration, anchor["profile"], programs, anchor["repetitions"])
    cases = build_cases(configuration, anchor["profile"], programs, anchor["repetitions"])
    case_ids = {case["case_id"] for case in cases}
    for platform in platforms:
        found.setdefault(platform, dict(schema_version=1, rows=[], provenance=[]))
        retained_comparison_rows(found[platform], cases)
        if extend:
            if frequency is not None and set(found[platform].get("reused_records", {})) - case_ids:
                raise ValueError("The continuation frequency grid must include every saved selected-software case. "
                                 "Widen the grid to preserve existing measurements")
            validate_reused_records(found[platform], cases,
                read_json(ROOT / "sources.lock.json") if sources is None else sources,
                platform={"ubuntu-latest": "Linux", "windows-latest": "Windows", "macos-latest": "Darwin"}[platform])
    current_run = int(os.environ.get("GITHUB_RUN_ID", "0"))
    return dict(mode=mode, platforms=[p for p in PLATFORMS if p in platforms],
        configuration=configuration, profile=anchor["profile"], software=",".join(programs),
        repetitions=anchor["repetitions"], source_ref=revision, planned_cases=len(cases),
        resume_run_id=requested, resume_case_order="",
        recorded_by_platform={p: len(found[p].get("reused_records", {})) for p in platforms},
        resume_artifacts={p: f"retained-{p}" for p in platforms},
        resume_artifact_run_ids={p: current_run for p in platforms},
        retained_by_platform={p: len(found[p]["rows"]) for p in platforms}, _retained_payloads=found)


def compact_provenance(sidecar):
    """Keep timing evidence without copying a reference request plan at each handoff."""
    proofs = []
    for original in sidecar["provenance"]:
        proof = dict(original)
        if "reference_requests" in proof.get("manifest", {}):
            manifest = dict(proof["manifest"])
            requests = manifest.pop("reference_requests")
            # Reference plans are not part of the timing manifest identity.
            # The artifact digest still identifies the full original archive.
            proof.update(manifest=manifest, reference_requests_fingerprint=fingerprint(requests))
        proofs.append(proof)
    return dict(sidecar, provenance=proofs)


def write_baselines(result, directory, repository, run_id, sources=None):
    """Save comparison data for every platform before sequential timing starts."""
    sources = read_json(ROOT / "sources.lock.json") if sources is None else sources
    payloads = result.pop("_retained_payloads")
    fields = ("configuration", "profile", "software", "repetitions", "source_ref", "planned_cases")
    settings = {key: result[key] for key in fields}
    settings["sources_fingerprint"] = fingerprint(sources)
    for platform, retained in payloads.items():
        retained = compact_provenance(retained)
        target = directory / platform
        cases = build_cases(settings["configuration"], settings["profile"],
                            settings["software"].split(","), settings["repetitions"])
        seeds = validate_reused_records(retained, cases, sources,
            platform={"ubuntu-latest": "Linux", "windows-latest": "Windows", "macos-latest": "Darwin"}[platform])
        write_retained(target / "retained-results.json", retained)
        for identity, record in seeds.items():
            atomic_json(target / "records" / f"{identity}.json", record)
        atomic_json(target / "refresh-campaign.json", dict(schema_version=1, repository=repository,
            run_id=int(run_id), platform=platform, resumed_from_run_id=int(result["resume_run_id"]),
            settings=settings, sources=sources, retained_fingerprint=fingerprint(retained),
            reused_count=len(seeds)))
