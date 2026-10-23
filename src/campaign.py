"""Resolve editable campaign inputs or recover an immutable timing continuation."""

from __future__ import annotations

import argparse
import copy
from decimal import Decimal, InvalidOperation, localcontext
import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import sys
import zipfile

from benchmark import BOUNDARIES, CAPABILITIES, ROOT, STATUSES, atomic_json, build_cases, fingerprint, read_json
from publish_progress import GitHub, PLATFORMS


MAX_CASES = 250_000
MAX_JSON_BYTES = 128 * 1024 * 1024
MAX_RECORD_BYTES = 8 * 1024 * 1024
MAX_SELECTED_BYTES = 512 * 1024 * 1024
IDENTITY_FIELDS = ("configuration", "sources", "profile", "cases", "adapter_hashes",
                   "platform", "architecture", "environment_fingerprint", "workload")
OUTPUT_FIELDS = ("mode", "platforms", "configuration", "profile", "software", "repetitions",
                 "source_ref", "resume_artifacts", "resume_run_id")


def decimal_number(value, name):
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite number")
    try:
        result = Decimal(str(value).strip())
    except InvalidOperation:
        raise ValueError(f"{name} must be a finite number") from None
    if not result.is_finite():
        raise ValueError(f"{name} must be a finite number")
    return result


def frequency_hz(value, name):
    number = decimal_number(value, name)
    if not math.isfinite(float(number)) or float(number) <= 0:
        raise ValueError(f"{name} must be finite and positive")
    with localcontext() as context:
        context.prec = max(28, len(number.as_tuple().digits) + 3)
        result = number * 1000
    if result <= 0 or result != result.to_integral_value():
        raise ValueError(f"{name} must be positive and represent an integer number of Hz")
    return int(result)


def csv_values(value, name, convert=str):
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a comma-separated list")
    items = [item.strip() for item in value.split(",")]
    if not items or any(not item for item in items):
        raise ValueError(f"{name} must contain a nonempty list without empty entries")
    converted = [convert(item) for item in items]
    if len(converted) != len(set(converted)):
        raise ValueError(f"{name} must contain unique values")
    return converted


def numeric_list(value, name):
    values = csv_values(value, name, lambda item: float(decimal_number(item, name)))
    if any(not math.isfinite(item) for item in values):
        raise ValueError(f"{name} must contain finite numbers")
    return values


def validate_plan_size(configuration, profile, software, repetitions):
    selection = configuration["profiles"][profile]
    frequency = configuration["frequency"]
    start, stop, step = (frequency[key] for key in ("start_hz", "stop_hz", "step_hz"))
    if any(isinstance(value, bool) or not isinstance(value, (int, float)) or
           not math.isfinite(value) or value <= 0 or int(value) != value for value in (start, stop, step)):
        raise ValueError("Frequency settings must be positive integer Hz")
    if start > stop:
        raise ValueError("frequency_start_khz must not exceed frequency_end_khz")
    count = (int(stop) - int(start)) // int(step) + 1
    if frequency.get("include_stop", True) and (int(stop) - int(start)) % int(step):
        count += 1
    for values in (selection.get("aspect_ratios", configuration["geometry"]["aspect_ratios"]),
                   selection.get("incidences_deg", configuration["incidences_deg"]),
                   selection.get("boundaries", configuration["boundaries"]), software):
        count *= len(values)
    count *= repetitions
    if count < 1 or count > MAX_CASES:
        raise ValueError(f"A campaign must contain between 1 and {MAX_CASES:,} cases. Narrow the selected grid")


def new_campaign(inputs, base):
    configuration = copy.deepcopy(base)
    programs = csv_values(inputs.get("software", "acousticts,prol_spheroid"), "software", str.lower)
    if set(programs) - (CAPABILITIES.keys() - {"acousticscattering"}):
        raise ValueError("Unknown timing software. AcousticScattering is reserved for numerical references")
    repetitions = decimal_number(inputs.get("repetitions", 1), "repetitions")
    if repetitions not in (1, 3, 5):
        raise ValueError("repetitions must be 1, 3, or 5")
    ratios = numeric_list(inputs.get("aspect_ratios", "10"), "aspect_ratios")
    if any(value <= 1 for value in ratios):
        raise ValueError("Every prolate aspect ratio must exceed one")
    angles = numeric_list(inputs.get("incidences_deg", "90"), "incidences_deg")
    if any(not 0 <= value <= 90 for value in angles):
        raise ValueError("Incidences must be between 0 and 90 degrees from the major axis")
    boundaries = csv_values(inputs.get("boundaries", "gas"), "boundaries")
    if set(boundaries) - set(BOUNDARIES):
        raise ValueError("Unknown boundary condition")
    major = float(decimal_number(inputs.get("semimajor_m", .07), "semimajor_m"))
    if not math.isfinite(major) or major <= 0:
        raise ValueError("semimajor_m must be finite and positive")
    configuration.update(geometry=dict(semimajor_m=major, aspect_ratios=ratios),
        incidences_deg=angles, boundaries=boundaries, software=programs, repetitions=int(repetitions),
        frequency=dict(start_hz=frequency_hz(inputs.get("frequency_start_khz", 1), "frequency_start_khz"),
                       stop_hz=frequency_hz(inputs.get("frequency_end_khz", 300), "frequency_end_khz"),
                       step_hz=frequency_hz(inputs.get("frequency_interval_khz", 2), "frequency_interval_khz"),
                       include_stop=True))
    configuration.setdefault("profiles", {})["custom"] = {}
    validate_plan_size(configuration, "custom", programs, int(repetitions))
    cases = build_cases(configuration, "custom", programs, int(repetitions))
    return configuration, programs, int(repetitions), len(cases)


def artifact_platform(name):
    for platform in PLATFORMS:
        if name == f"results-{platform}" or re.fullmatch(rf"checkpoint-{re.escape(platform)}-\d+-\d+", name):
            return platform
    return None


def select_resume_artifacts(artifacts, explicit=""):
    candidates = [item for item in artifacts if not item.get("expired", False)
                  and artifact_platform(item.get("name", ""))]
    newest = lambda item: (item.get("created_at", ""), item["name"].startswith("results-"), int(item["id"]))
    if explicit:
        selected = max((item for item in candidates if item["name"] == explicit), key=newest, default=None)
        if selected is None:
            raise ValueError("resume_artifact must name an available timing result or checkpoint artifact")
        return {artifact_platform(explicit): selected}
    result = {}
    for platform in PLATFORMS:
        # A later attempt can have a newer checkpoint but no final artifact yet.
        options = [item for item in candidates if artifact_platform(item["name"]) == platform]
        if options:
            result[platform] = max(options, key=newest)
    if not result:
        raise ValueError("The selected run has no available timing checkpoints")
    return result


def _json(content):
    def invalid(value):
        raise ValueError(f"Nonfinite JSON constant: {value}")
    value = json.loads(content, parse_constant=invalid)
    json.dumps(value, allow_nan=False)
    return value


def read_checkpoint(content):
    """Read only bounded JSON metadata and case records, without extracting files."""
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        entries = archive.infolist()
        if len(entries) > MAX_CASES * 4 + 1000:
            raise ValueError("Timing artifact contains too many files")
        for entry in entries:
            path = PurePosixPath(entry.filename)
            if path.is_absolute() or ".." in path.parts or "\\" in entry.filename or ":" in entry.filename:
                raise ValueError("Unsafe timing artifact path")
            if (entry.external_attr >> 16) & 0o170000 == 0o120000:
                raise ValueError("Timing artifacts must not contain symbolic links")
        manifests = [entry for entry in entries if PurePosixPath(entry.filename).name == "manifest.json"]
        if len(manifests) != 1:
            raise ValueError("Timing artifact must contain exactly one manifest")
        prefix = PurePosixPath(manifests[0].filename).parent
        values, records, total = {}, {}, 0
        for entry in entries:
            path = PurePosixPath(entry.filename)
            record = path.parent == prefix / "records" and path.suffix == ".json"
            metadata = path.parent == prefix and path.name in (
                "manifest.json", "status.json", "results.json", "benchmark-source.json")
            if not metadata and not record:
                continue
            maximum = MAX_RECORD_BYTES if record else MAX_JSON_BYTES
            total += entry.file_size
            if entry.file_size > maximum or total > MAX_SELECTED_BYTES:
                raise ValueError("Timing artifact JSON exceeds the supported size")
            target, key = (records, path.stem) if record else (values, path.name)
            if key in target or (record and not re.fullmatch(r"[0-9a-f]{64}", key)):
                raise ValueError("Duplicate or invalid timing artifact record")
            target[key] = _json(archive.read(entry))
    if not {"manifest.json", "status.json", "results.json"} <= values.keys():
        raise ValueError("Timing artifact lacks its manifest, status, or complete case table")
    return values, records


def checkpoint_settings(content, repository, head_sha, platform):
    values, records = read_checkpoint(content)
    manifest, status, rows = (values[name] for name in ("manifest.json", "status.json", "results.json"))
    if not isinstance(manifest, dict) or not isinstance(status, dict) or not isinstance(rows, list) or not rows:
        raise ValueError("Invalid timing manifest, status, or case table")
    if any(not isinstance(row, dict) for row in rows) or len(rows) > MAX_CASES:
        raise ValueError("Invalid timing case table")
    if set(IDENTITY_FIELDS) - manifest.keys() or fingerprint(
            {key: manifest[key] for key in IDENTITY_FIELDS}) != manifest.get("fingerprint"):
        raise ValueError("Timing manifest fingerprint is invalid")
    if manifest["platform"] != {"ubuntu-latest": "Linux", "windows-latest": "Windows", "macos-latest": "Darwin"}[platform]:
        raise ValueError("Timing artifact platform does not match its manifest")
    programs = list(dict.fromkeys(row.get("software") for row in rows))
    if any(name not in CAPABILITIES or name == "acousticscattering" for name in programs):
        raise ValueError("Continuation contains unsupported timing software")
    counts = {row.get("repetition") for row in rows}
    if any(type(value) is not int or value < 1 for value in counts):
        raise ValueError("Continuation contains invalid repetitions")
    repetitions = max(counts)
    if counts != set(range(1, repetitions + 1)):
        raise ValueError("Continuation repetitions are incomplete")
    configuration, profile = manifest["configuration"], manifest["profile"]
    validate_plan_size(configuration, profile, programs, repetitions)
    cases = build_cases(configuration, profile, programs, repetitions)
    identities = [case["case_id"] for case in cases]
    if identities != manifest["cases"] or identities != [row.get("case_id") for row in rows]:
        raise ValueError("Continuation case identities or software order do not match the manifest")
    for case, row in zip(cases, rows):
        if any(row.get(key) != value for key, value in case.items()):
            raise ValueError("Continuation case inputs do not match the manifest")
        if row.get("status") not in STATUSES | {"pending"}:
            raise ValueError("Continuation contains an unknown case status")
    recorded = {row["case_id"]: row for row in rows if row["status"] != "pending"}
    if (type(status.get("recorded")) is not int or status["recorded"] != len(recorded) or
            status.get("planned") != len(cases) or records.keys() != recorded.keys()):
        raise ValueError("Continuation artifact must contain every recorded case")
    for identity, record in records.items():
        row = recorded[identity]
        if (not isinstance(record, dict) or record.get("case_id") != identity or
                record.get("source_sha") != manifest["sources"]["sources"][row["software"]]["sha"] or
                any(row.get(key) != value for key, value in record.items())):
            raise ValueError("Continuation records do not match the case table and pinned source")
    source = values.get("benchmark-source.json")
    if source is not None:
        if not isinstance(source, dict) or source.get("schema_version") != 1 or source.get("repository") != repository:
            raise ValueError("Continuation source metadata belongs to a different repository or schema")
        revision = source.get("source_ref", "")
    else:
        revision = head_sha
    if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Continuation source must be an exact commit SHA")
    return dict(configuration=configuration, profile=profile, software=",".join(programs),
                repetitions=repetitions, source_ref=revision, planned_cases=len(cases),
                recorded_cases=len(records), sources_fingerprint=fingerprint(manifest["sources"]))


def resume_campaign(inputs, repository, api):
    run_id = str(inputs.get("resume_run_id", "")).strip()
    if not run_id.isdecimal() or int(run_id) < 1:
        raise ValueError("Resume mode requires a positive resume_run_id")
    run = api.request("GET", f"/actions/runs/{run_id}")
    if (run.get("repository", {}).get("full_name") != repository or
            (run.get("head_repository") or {}).get("full_name") != repository or
            run.get("event") != "workflow_dispatch" or run.get("status") != "completed" or
            run.get("path") != ".github/workflows/benchmark.yml"):
        raise ValueError("Resume requires this repository's completed timing workflow run")
    artifacts = []
    for page in range(1, 101):
        part = api.request("GET", f"/actions/runs/{run_id}/artifacts?per_page=100&page={page}")["artifacts"]
        artifacts.extend(part)
        if len(part) < 100:
            break
    else:
        raise ValueError("Too many artifacts to resolve this continuation")
    selected = select_resume_artifacts(artifacts, str(inputs.get("resume_artifact", "")).strip())
    shared, recorded = None, {}
    for platform, artifact in selected.items():
        settings = checkpoint_settings(api.archive(artifact["id"]), repository, run.get("head_sha", ""), platform)
        recorded[platform] = settings.pop("recorded_cases")
        if shared is not None and fingerprint(settings) != fingerprint(shared):
            raise ValueError("Selected platforms have different configurations, software selections, or source revisions")
        shared = settings
    shared.pop("sources_fingerprint")
    return dict(shared, mode="resume", platforms=list(selected), resume_run_id=run_id,
                resume_artifacts={platform: artifact["name"] for platform, artifact in selected.items()},
                recorded_by_platform=recorded)


def resolve(inputs, configuration, repository, revision, api=None):
    """Return validated settings without preparing software or running a worker."""
    if not isinstance(inputs, dict):
        raise ValueError("CAMPAIGN_INPUTS must contain a JSON object")
    mode = inputs.get("mode", "new")
    if mode not in ("new", "resume", "prepare"):
        raise ValueError("mode must be new, resume, or prepare")
    if mode == "resume":
        if api is None:
            api = GitHub(repository, os.environ.get("GITHUB_TOKEN"), timeout_seconds=180)
        return resume_campaign(inputs, repository, api)
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("New campaigns require an exact benchmark source SHA")
    platform = inputs.get("platform", "ubuntu-latest")
    platform = {"ubuntu": "ubuntu-latest", "windows": "windows-latest",
                "mac": "macos-latest", "macos": "macos-latest"}.get(platform, platform)
    if platform not in (*PLATFORMS, "all"):
        raise ValueError("Unknown runner platform")
    configuration, programs, repetitions, planned = new_campaign(inputs, configuration)
    return dict(mode=mode, platforms=list(PLATFORMS) if platform == "all" else [platform],
                configuration=configuration, profile="custom", software=",".join(programs),
                repetitions=repetitions, source_ref=revision, resume_artifacts={}, resume_run_id="",
                planned_cases=planned, recorded_by_platform={})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Resolved campaign settings JSON")
    parser.add_argument("--config", type=Path, default=ROOT / "benchmark.json")
    args = parser.parse_args()
    inputs = _json(os.environ.get("CAMPAIGN_INPUTS", "{}"))
    repository = os.environ["GITHUB_REPOSITORY"]
    api = (GitHub(repository, os.environ.get("GITHUB_TOKEN"), timeout_seconds=180)
           if isinstance(inputs, dict) and inputs.get("mode") == "resume" else None)
    result = resolve(inputs, read_json(args.config), repository, os.environ.get("GITHUB_SHA", ""), api)
    atomic_json(args.output, result)
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            for name in OUTPUT_FIELDS:
                value = result[name]
                text = json.dumps(value, separators=(",", ":"), allow_nan=False) if isinstance(value, (dict, list)) else str(value)
                if "\n" in text or "\r" in text:
                    raise ValueError(f"Workflow output {name} must fit on one line")
                output.write(f"{name}={text}\n")
    completed = sum(result["recorded_by_platform"].values())
    print(f"{result['mode']}: {result['planned_cases']} cases per platform, "
          f"{result['software']}, platforms={','.join(result['platforms'])}, "
          f"{completed} preserved records. Resolved settings: {args.output}")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError, RuntimeError, zipfile.BadZipFile) as error:
        print(f"Campaign resolution failed: {error}", file=sys.stderr)
        raise SystemExit(2)
