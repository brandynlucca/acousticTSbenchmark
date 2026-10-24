"""Restart selected software while preserving completed comparison measurements."""

from __future__ import annotations

import copy
import hashlib
import io
import os
from pathlib import PurePosixPath
import re
import zipfile

from benchmark import atomic_json, build_cases, fingerprint, read_json, ROOT, summarize
from publish_progress import PLATFORMS, retained_comparison_rows


def baseline_settings(content, repository, run_id, platform):
    """Recognize an immutable refresh baseline saved before platform setup."""
    from campaign import MAX_JSON_BYTES, _json, validate_plan_size
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        names = archive.namelist()
        if "manifest.json" in names or "refresh-campaign.json" not in names:
            return None
        if set(names) - {"refresh-campaign.json", "retained-results.json", "benchmark-source.json"}:
            raise ValueError("Unexpected files in a refresh baseline")
        if len(names) != len(set(names)) or "retained-results.json" not in names:
            raise ValueError("Incomplete or duplicate refresh baseline metadata")
        values = {}
        for name in ("refresh-campaign.json", "retained-results.json"):
            entry = archive.getinfo(name)
            if entry.file_size > MAX_JSON_BYTES or (entry.external_attr >> 16) & 0o170000 == 0o120000:
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
    return dict(settings, recorded_cases=0)


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


def resolve_refresh(inputs, repository, revision, api):
    from campaign import (CAPABILITIES, ORDER_CHOICES, artifact_platform, checkpoint_settings,
                          csv_values, read_checkpoint, select_resume_artifacts,
                          setup_only_checkpoint, timing_or_restore_started)
    requested = str(inputs.get("resume_run_id", "")).strip()
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
        selected = select_resume_artifacts(artifacts)
        if platforms is None:
            platforms = {p for p in PLATFORMS if any(job.get("name") == f"benchmark ({p})" for job in jobs)} | set(selected)
        parents = set()
        for platform, artifact in selected.items():
            content = api.archive(artifact["id"])
            baseline = baseline_settings(content, repository, current, platform)
            if baseline is not None:
                meta, retained = _read_baseline(content)
                settings = baseline
                source = dict(resumed_from_run_id=meta.get("resumed_from_run_id"))
                rows, provenance = retained["rows"], retained["provenance"]
            else:
                source = setup_only_checkpoint(content, repository, current)
                if source is not None:
                    parent = source.get("resumed_from_run_id")
                    if parent is not None:
                        parents.add(parent)
                    continue
                settings = checkpoint_settings(content, repository, run.get("head_sha", ""), platform,
                                               allow_unexported=run.get("conclusion") == "cancelled")
                values, records = read_checkpoint(content)
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
    cases = build_cases(configuration, anchor["profile"], programs, anchor["repetitions"])
    for platform in platforms:
        found.setdefault(platform, dict(schema_version=1, rows=[], provenance=[]))
        retained_comparison_rows(found[platform], cases)
    current_run = int(os.environ.get("GITHUB_RUN_ID", "0"))
    return dict(mode="refresh", platforms=[p for p in PLATFORMS if p in platforms],
        configuration=configuration, profile=anchor["profile"], software=",".join(programs),
        repetitions=anchor["repetitions"], source_ref=revision, planned_cases=len(cases),
        resume_run_id=requested, resume_case_order="", recorded_by_platform={p: 0 for p in platforms},
        resume_artifacts={p: f"retained-{p}" for p in platforms},
        resume_artifact_run_ids={p: current_run for p in platforms},
        retained_by_platform={p: len(found[p]["rows"]) for p in platforms}, _retained_payloads=found)


def write_baselines(result, directory, repository, run_id, sources=None):
    """Save comparison data for every platform before sequential timing starts."""
    sources = read_json(ROOT / "sources.lock.json") if sources is None else sources
    payloads = result.pop("_retained_payloads")
    fields = ("configuration", "profile", "software", "repetitions", "source_ref", "planned_cases")
    settings = {key: result[key] for key in fields}
    settings["sources_fingerprint"] = fingerprint(sources)
    for platform, retained in payloads.items():
        target = directory / platform
        atomic_json(target / "retained-results.json", retained)
        atomic_json(target / "refresh-campaign.json", dict(schema_version=1, repository=repository,
            run_id=int(run_id), platform=platform, resumed_from_run_id=int(result["resume_run_id"]),
            settings=settings, sources=sources, retained_fingerprint=fingerprint(retained)))
