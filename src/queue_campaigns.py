"""Advance a saved benchmark sequence only after its preceding run finishes."""

import argparse
import base64
from datetime import datetime, timedelta, timezone
import json
import os
import re
import sys
import time
from urllib.parse import quote

from campaign import artifact_platform, checkpoint_settings, select_resume_artifacts
from publish_progress import APIError, BRANCH, GitHub, PLATFORMS


QUEUE_FILE = "campaign-queue.json"
QUEUE_WORKFLOW = "queue.yml"
BENCHMARK_WORKFLOW = ".github/workflows/benchmark.yml"
MAX_QUEUE_BYTES = 1024 * 1024
SOFTWARE = {"acousticts", "prol_spheroid", "echosms", "liquid_spheroid"}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def request_platform(request):
    inputs = request["inputs"]
    platform = inputs.get("continuation_platform") if inputs.get("mode") in {"extend", "refresh"} else inputs.get("platform")
    if platform not in PLATFORMS:
        raise ValueError("Finishing a queued software request requires one explicit runner platform")
    return platform


def receipt_run_ids(state):
    return {receipt["run_id"] for current in state["dispatched"].values()
            for receipt in [*current.get("history", []), current]}


def validate_state(state, repository):
    """Reject altered repositories, nonsequential receipts, and unresolved parents."""
    if isinstance(state, dict) and state.get("schema_version") == 2:
        return validate_parallel_state(state, repository)
    if (not isinstance(state, dict) or state.get("schema_version") != 1 or
            state.get("repository") != repository or
            state.get("status") not in {"active", "paused", "blocked", "complete"} or
            not re.fullmatch(r"[a-f0-9]{40}", state.get("source_ref", "")) or
            type(state.get("initial_run_id")) is not int or state["initial_run_id"] < 1 or
            not isinstance(state.get("requests"), list) or not 1 <= len(state["requests"]) <= 100 or
            not isinstance(state.get("dispatched"), dict)):
        raise ValueError("Invalid benchmark queue metadata")
    ref = state.get("dispatch_ref", "main")
    if not isinstance(ref, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", ref):
        raise ValueError("Queue dispatch ref must be main or an exact tag name")
    initial_key = state.get("initial_key", "acousticts:0.01")
    if not isinstance(initial_key, str) or not re.fullmatch(r"[a-z0-9_]+:0\.(?:001|01|07|1|5)", initial_key):
        raise ValueError("Invalid initial benchmark identity")
    recovery = state.get("recovery")
    if recovery is not None and (not isinstance(recovery, dict) or
            recovery.get("run_id") != state["initial_run_id"] or
            not isinstance(recovery.get("reason"), str) or not recovery["reason"].strip()):
        raise ValueError("Recovery must identify the initial failed run and its repair")
    seen = {initial_key}
    pending = False
    run_ids = {state["initial_run_id"]}
    for request in state["requests"]:
        if (not isinstance(request, dict) or request.get("software") not in SOFTWARE or
                request.get("semimajor_m") not in {"0.001", "0.01", "0.07", "0.1", "0.5"} or
                request.get("key") != f"{request['software']}:{request['semimajor_m']}" or
                request["key"] in seen or
                request.get("parent") is not None and request["parent"] not in seen or
                not isinstance(request.get("inputs"), dict)):
            raise ValueError("Invalid benchmark queue request or parent ordering")
        inputs = request["inputs"]
        parent = request.get("parent")
        baseline = request.get("baseline_run_id")
        if ("baseline_run_id" in request and
                (parent is not None or type(baseline) is not int or baseline < 1)):
            raise ValueError("A saved refresh baseline requires a positive run ID and no queue parent")
        mode = "extend" if parent else "refresh" if baseline is not None else "new"
        if (parent is not None and parent.split(":")[1] != request["semimajor_m"] or
                inputs.get("mode") != mode or
                inputs.get("software" if mode == "new" else "refresh_software") != request["software"] or
                mode == "new" and inputs.get("semimajor_m") != request["semimajor_m"] or
                "resume_run_id" in inputs):
            raise ValueError("Queue inputs do not match the saved software and size")
        if type(request.get("finish_before_next", False)) is not bool:
            raise ValueError("finish_before_next must be a boolean")
        if request.get("finish_before_next") or baseline is not None:
            request_platform(request)
        receipt = state["dispatched"].get(request["key"])
        if receipt is None:
            pending = True
        elif (pending or not isinstance(receipt, dict) or
              type(receipt.get("run_id")) is not int or receipt["run_id"] < 1 or
              receipt["run_id"] in run_ids or not isinstance(receipt.get("inputs"), dict)):
            raise ValueError("Queue dispatch receipts must form an ordered prefix")
        else:
            history = receipt.get("history", [])
            if not isinstance(history, list):
                raise ValueError("Invalid benchmark continuation history")
            for previous in [*history, receipt]:
                if (not isinstance(previous, dict) or type(previous.get("run_id")) is not int or
                        previous["run_id"] < 1 or previous["run_id"] in run_ids or
                        not isinstance(previous.get("inputs"), dict) or
                        previous is not receipt and "history" in previous or
                        "recorded_before" in previous and
                        (type(previous["recorded_before"]) is not int or previous["recorded_before"] < 0) or
                        "source_ref" in previous and (not isinstance(previous["source_ref"], str) or
                            not re.fullmatch(r"[a-f0-9]{40}", previous["source_ref"]))):
                    raise ValueError("Invalid or duplicate benchmark continuation receipt")
                run_ids.add(previous["run_id"])
        seen.add(request["key"])
    if set(state["dispatched"]) - {request["key"] for request in state["requests"]}:
        raise ValueError("Unexpected benchmark queue receipt")
    intent = state.get("dispatch_intent")
    if intent is not None:
        index = len(state["dispatched"])
        continuing = isinstance(intent, dict) and "continuation_of" in intent
        if continuing:
            index -= 1
        if (not isinstance(intent, dict) or index >= len(state["requests"]) or
                index < 0 or
                intent.get("key") != state["requests"][index]["key"] or
                intent.get("source_ref") != state["source_ref"] or
                not isinstance(intent.get("inputs"), dict)):
            raise ValueError("Invalid pending dispatch intent")
        if continuing:
            request = state["requests"][index]
            receipt = state["dispatched"][request["key"]]
            inputs = intent["inputs"]
            if (not request.get("finish_before_next") or intent["continuation_of"] != receipt["run_id"] or
                    type(intent.get("recorded_before")) is not int or intent["recorded_before"] < 0 or
                    inputs.get("mode") != "extend" or inputs.get("refresh_software") != request["software"] or
                    inputs.get("resume_run_id") != str(receipt["run_id"]) or
                    inputs.get("continuation_platform") != request_platform(request)):
                raise ValueError("Invalid pending software completion intent")
        elif intent["inputs"] != resolved_inputs(state, state["requests"][index]):
            raise ValueError("Pending dispatch inputs do not match the saved request")
        datetime.fromisoformat(intent["created_at"])


def load_queue(api):
    entry = api.request("GET", f"/contents/{QUEUE_FILE}?ref={BRANCH}")
    if entry.get("encoding") != "base64" or entry.get("size", 0) > MAX_QUEUE_BYTES:
        raise ValueError("Unsupported benchmark queue content")
    content = base64.b64decode("".join(entry["content"].split()), validate=True)
    if len(content) > MAX_QUEUE_BYTES:
        raise ValueError("Benchmark queue exceeds its size limit")
    state = json.loads(content)
    validate_state(state, api.repository)
    return state, entry["sha"]


def save_queue(api, state, expected_sha):
    """Use content SHA checks so a competing controller cannot dispatch twice."""
    validate_state(state, api.repository)
    raw = json.dumps(state, allow_nan=False, indent=2).encode()
    if len(raw) > MAX_QUEUE_BYTES:
        raise ValueError("Benchmark queue exceeds its size limit")
    body = dict(message="Advance saved benchmark sequence", branch=BRANCH,
                sha=expected_sha, content=base64.b64encode(raw).decode())
    for _ in range(4):
        try:
            result = api.request("PUT", f"/contents/{QUEUE_FILE}", body)
            return result["content"]["sha"]
        except APIError as error:
            if error.status not in (409, 422):
                raise
            _, current_sha = load_queue(api)
            if current_sha != expected_sha:
                raise RuntimeError("Benchmark queue changed concurrently. No dispatch was retried") from None
    raise RuntimeError("Data branch changed too often to save the benchmark queue")


def checked_run(api, run_id):
    run = api.request("GET", f"/actions/runs/{run_id}")
    if (run.get("id") != run_id or run.get("event") != "workflow_dispatch" or
            run.get("path") != BENCHMARK_WORKFLOW or
            run.get("repository", {}).get("full_name") != api.repository or
            run.get("head_repository", {}).get("full_name") != api.repository):
        raise ValueError("Queue predecessor is not a benchmark dispatched in this repository")
    return run


def has_saved_baseline(api, run_id):
    for page in range(1, 11):
        entries = api.request("GET", f"/actions/runs/{run_id}/artifacts?per_page=100&page={page}")["artifacts"]
        if any(not entry.get("expired", False) and artifact_platform(entry.get("name", ""))
               for entry in entries):
            return True
        if len(entries) < 100:
            return False
    raise ValueError("Too many artifacts to establish the predecessor baseline")


def saved_checkpoint_settings(api, run, platform):
    """Read the exact requested platform before using a saved timing baseline."""
    artifacts = []
    for page in range(1, 11):
        entries = api.request("GET", f"/actions/runs/{run['id']}/artifacts?per_page=100&page={page}")["artifacts"]
        artifacts.extend(entries)
        if len(entries) < 100:
            break
    else:
        raise ValueError("Too many artifacts to verify completed predictions")
    selected = select_resume_artifacts(artifacts, platform=platform)[platform]
    content = api.archive(selected["id"])
    from refresh import baseline_settings
    baseline = baseline_settings(content, api.repository, run["id"], platform)
    return (baseline if baseline is not None else
            checkpoint_settings(content, api.repository, run.get("head_sha", ""), platform))


def completion_progress(api, run, request, source_ref):
    """Count immutable saved predictions for the exact requested software and size."""
    settings = saved_checkpoint_settings(api, run, request_platform(request))
    if (settings["software"] != request["software"] or
            settings["configuration"]["geometry"]["semimajor_m"] != float(request["semimajor_m"]) or
            settings["source_ref"] != source_ref):
        raise ValueError("Saved checkpoint does not match the queued software, semimajor axis, and source")
    return settings["recorded_cases"], settings["planned_cases"]


def resolved_inputs(state, request):
    inputs = dict(request["inputs"])
    if request.get("parent"):
        identities = {state.get("initial_key", "acousticts:0.01"): state["initial_run_id"],
                      **{key: value["run_id"] for key, value in state["dispatched"].items()}}
        inputs["resume_run_id"] = str(identities[request["parent"]])
    elif request.get("baseline_run_id") is not None:
        inputs["resume_run_id"] = str(request["baseline_run_id"])
    return inputs


def expected_title(inputs):
    if inputs["mode"] == "refresh":
        return f"Refresh {inputs['refresh_software']} from benchmark {inputs['resume_run_id']}"
    if inputs["mode"] == "extend":
        return f"Continue {inputs['refresh_software']} after benchmark {inputs['resume_run_id']}"
    return f"New {inputs['software']} benchmark (a={inputs['semimajor_m']} m, {inputs.get('platform', 'ubuntu-latest')})"


def pause(api, state, sha, reason, status="paused"):
    state.update(status=status, reason=reason, updated_at=utc_now())
    save_queue(api, state, sha)
    return dict(status=status, reason=reason)


def record_receipt(api, state, sha, identity, inputs):
    # GitHub may return the new ID before its title and commit metadata settle.
    # Retry only the read, never the accepted dispatch.
    for delay in (0, 1, 2, 4, 8):
        if delay:
            time.sleep(delay)
        run = checked_run(api, identity)
        if run.get("head_sha") == state["source_ref"] and run.get("display_title") == expected_title(inputs):
            break
    else:
        return pause(api, state, sha, "Dispatched run does not match the pinned queue request", "blocked")
    intent = state["dispatch_intent"]
    key = intent["key"]
    receipt = dict(run_id=identity,
        url=f"https://github.com/{api.repository}/actions/runs/{identity}", inputs=inputs,
        source_ref=state["source_ref"])
    if "continuation_of" in intent:
        previous = state["dispatched"][key]
        receipt.update(recorded_before=intent["recorded_before"],
                       history=[*previous.get("history", []),
                                {name: value for name, value in previous.items() if name != "history"}])
    state["dispatched"][key] = receipt
    state.pop("dispatch_intent")
    state.pop("reason", None)
    state.update(status="active", updated_at=utc_now())
    save_queue(api, state, sha)
    return dict(status="dispatched", key=key, run_id=identity)


def recover_intent(api, state, sha):
    """Adopt a uniquely identified dispatch, never repeat an uncertain POST."""
    intent = state["dispatch_intent"]
    started = datetime.fromisoformat(intent["created_at"])
    matches = []
    runs = api.request("GET", "/actions/workflows/benchmark.yml/runs?event=workflow_dispatch&per_page=100")["workflow_runs"]
    for run in runs:
        created = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
        if (started - timedelta(seconds=2) <= created <= started + timedelta(minutes=5) and
                run.get("head_sha") == intent["source_ref"] and
                run.get("display_title") == expected_title(intent["inputs"]) and
                run.get("id") not in receipt_run_ids(state)):
            matches.append(run["id"])
    if len(matches) != 1:
        return pause(api, state, sha,
                     "Dispatch receipt is uncertain. Resolve the saved intent before issuing another dispatch",
                     "blocked")
    return record_receipt(api, state, sha, matches[0], intent["inputs"])


PARALLEL_WORKFLOW = ".github/workflows/parallel-spectra.yml"
PARALLEL_CONTROLLER_FILES = {"src/parallel_campaign.py", "src/publish_progress.py",
    "src/dataset.py", "src/report.py", "tests/test_parallel_campaign.py",
    "tests/test_publish_progress.py", "tests/test_dataset.py"}
PARALLEL_INPUTS = {"mode", "software", "semimajor_axes", "baseline_runs", "aspect_ratios",
    "incidences_deg", "boundaries", "frequency_start_khz", "frequency_end_khz",
    "frequency_interval_khz", "additional_repetitions", "max_parallel", "automatic_continue",
    "timing_budget_minutes", "campaign_id", "platform"}


def validate_parallel_state(state, repository):
    if (state.get("schema_version") != 2 or state.get("repository") != repository or
            state.get("status") not in {"active", "paused", "blocked", "complete"} or
            not isinstance(state.get("requests"), list) or not 1 <= len(state["requests"]) <= 100 or
            not isinstance(state.get("dispatched"), dict)):
        raise ValueError("Invalid parallel sequence metadata")
    keys, runs, missing = set(), set(), False
    for request in state["requests"]:
        if (not isinstance(request, dict) or not isinstance(request.get("key"), str) or
                not re.fullmatch(r"[a-z0-9_-]{1,80}", request["key"]) or request["key"] in keys or
                not isinstance(request.get("source_ref"), str) or
                not re.fullmatch(r"[a-f0-9]{40}", request["source_ref"]) or
                not isinstance(request.get("dispatch_ref"), str) or
                not re.fullmatch(r"[A-Za-z0-9_.-]+", request["dispatch_ref"]) or
                not isinstance(request.get("inputs"), dict)):
            raise ValueError("Invalid parallel sequence request")
        controllers = request.get("approved_controllers", [request["source_ref"]])
        if (not isinstance(controllers, list) or not 1 <= len(controllers) <= 20 or
                controllers[0] != request["source_ref"] or
                any(not isinstance(ref, str) or not re.fullmatch(r"[a-f0-9]{40}", ref)
                    for ref in controllers) or len(set(controllers)) != len(controllers)):
            raise ValueError("Invalid approved parallel controller chain")
        interrupted = request.get("interrupted_runs", [])
        if (not isinstance(interrupted, list) or len(interrupted) > 100 or
                any(type(run_id) is not int or run_id < 1 for run_id in interrupted) or
                len(set(interrupted)) != len(interrupted)):
            raise ValueError("Invalid interrupted parallel run list")
        keys.add(request["key"])
        inputs = request["inputs"]
        if (set(inputs) - PARALLEL_INPUTS or any(not isinstance(value, str) for value in inputs.values()) or
                inputs.get("platform") not in PLATFORMS or
                not inputs.get("max_parallel", "16").isdigit() or
                not 1 <= int(inputs.get("max_parallel", "16")) <= 16):
            raise ValueError("Invalid parallel workflow inputs")
        campaign = inputs.get("campaign_id", "")
        if campaign:
            if (not re.fullmatch(r"[1-9][0-9]*", campaign) or
                    set(inputs) - {"campaign_id", "platform", "max_parallel"}):
                raise ValueError("Resume inputs must retain the immutable campaign selection")
        elif (inputs.get("mode") not in {"repeat", "extend", "refresh"} or
                inputs.get("software") not in SOFTWARE or not inputs.get("semimajor_axes") or
                any(size.strip() not in {"0.001", "0.01", "0.07", "0.1", "0.5"}
                    for size in inputs["semimajor_axes"].split(",")) or
                not inputs.get("additional_repetitions", "1").isdigit() or
                not 1 <= int(inputs.get("additional_repetitions", "1")) <= 20 or
                inputs.get("automatic_continue") != "true"):
            raise ValueError("New stages require an explicit continuing parallel selection")
        receipt = state["dispatched"].get(request["key"])
        if receipt is None:
            if interrupted:
                raise ValueError("Interrupted parallel runs require an existing stage receipt")
            missing = True
            continue
        if (missing or not isinstance(receipt, dict) or type(receipt.get("run_id")) is not int or
                receipt["run_id"] < 1 or receipt["run_id"] in runs or
                receipt.get("inputs") != inputs or receipt.get("source_ref") != request["source_ref"] or
                receipt.get("campaign_id") != (campaign or str(receipt["run_id"]))):
            raise ValueError("Parallel receipts must form an ordered prefix")
        runs.add(receipt["run_id"])
    if set(state["dispatched"]) - keys:
        raise ValueError("Unexpected parallel dispatch receipt")
    intent = state.get("dispatch_intent")
    if intent is not None:
        index = len(state["dispatched"])
        if index >= len(state["requests"]) or not isinstance(intent, dict):
            raise ValueError("Invalid parallel dispatch intent")
        request = state["requests"][index]
        if (any(intent.get(key) != request[key] for key in ("key", "source_ref", "dispatch_ref", "inputs")) or
                intent.get("approved_controllers", [request["source_ref"]]) !=
                request.get("approved_controllers", [request["source_ref"]]) or
                intent.get("interrupted_runs", []) != request.get("interrupted_runs", [])):
            raise ValueError("Parallel dispatch intent differs from the saved request")
        try:
            created = datetime.fromisoformat(intent["created_at"])
        except (KeyError, TypeError, ValueError):
            raise ValueError("Invalid parallel dispatch timestamp") from None
        if created.utcoffset() is None:
            raise ValueError("Parallel dispatch timestamp requires a UTC offset")


def parallel_title(inputs):
    campaign = inputs.get("campaign_id")
    return (f"Continue spectra {campaign}" if campaign else
            f"{inputs['mode']} {inputs['software']} {inputs['platform']} spectra, a={inputs['semimajor_axes']} m")


def checked_parallel_run(api, run_id):
    run = api.request("GET", f"/actions/runs/{run_id}")
    if (run.get("id") != run_id or run.get("event") != "workflow_dispatch" or
            run.get("path") != PARALLEL_WORKFLOW or
            run.get("repository", {}).get("full_name") != api.repository or
            run.get("head_repository", {}).get("full_name") != api.repository):
        raise ValueError("Sequence run is not a parallel spectrum workflow in the expected repository")
    return run


def parallel_receipt(api, state, sha, identity):
    intent = state["dispatch_intent"]
    for delay in (0, 1, 2, 4, 8):
        if delay:
            time.sleep(delay)
        run = checked_parallel_run(api, identity)
        if run.get("head_sha") == intent["source_ref"] and run.get("display_title") == parallel_title(intent["inputs"]):
            break
    else:
        return pause(api, state, sha, "Parallel dispatch metadata differs from the pinned request", "blocked")
    campaign = intent["inputs"].get("campaign_id") or str(identity)
    state["dispatched"][intent["key"]] = dict(run_id=identity, campaign_id=campaign,
        inputs=dict(intent["inputs"]), source_ref=intent["source_ref"],
        url=f"https://github.com/{api.repository}/actions/runs/{identity}")
    state.pop("dispatch_intent")
    state.pop("reason", None)
    state.update(status="active", updated_at=utc_now())
    save_queue(api, state, sha)
    return dict(status="dispatched", key=intent["key"], run_id=identity, campaign_id=campaign)


def recover_parallel_intent(api, state, sha):
    intent = state["dispatch_intent"]
    started = datetime.fromisoformat(intent["created_at"])
    runs = api.request("GET", "/actions/workflows/parallel-spectra.yml/runs?event=workflow_dispatch&per_page=100")["workflow_runs"]
    recorded = {receipt["run_id"] for receipt in state["dispatched"].values()}
    matches = []
    for run in runs:
        created = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
        if (started-timedelta(seconds=2) <= created <= started+timedelta(minutes=5) and
                run.get("head_sha") == intent["source_ref"] and run.get("display_title") == parallel_title(intent["inputs"]) and
                run.get("id") not in recorded and run.get("path") == PARALLEL_WORKFLOW):
            matches.append(run["id"])
    if len(matches) != 1:
        return pause(api, state, sha, "Parallel dispatch receipt is uncertain. Resolve the saved intent before redispatch", "blocked")
    return parallel_receipt(api, state, sha, matches[0])


def parallel_state_document(api, campaign):
    from parallel_campaign import filename, get_document
    saved, _ = get_document(api, filename(campaign, "state"))
    return saved


def approved_parallel_controller(api, request, saved):
    """Verify applied controller changes without changing the original dispatch identity."""
    approved = request.get("approved_controllers", [request["source_ref"]])
    current = saved.get("controller_ref", request["source_ref"])
    if current not in approved:
        raise ValueError("Controller revision is not approved for the stage")
    applied = approved.index(current)
    history = saved.get("controller_history", [])
    if not isinstance(history, list):
        raise ValueError("Controller history is malformed")
    if history and (not isinstance(history[-1], dict) or history[-1].get("current") != current):
        raise ValueError("Controller history does not end at the active revision")
    origins = [index for index, entry in enumerate(history)
               if isinstance(entry, dict) and entry.get("previous") == approved[0]]
    if not applied:
        if origins:
            raise ValueError("Controller history returns to the original stage revision")
        return current
    if len(origins) != 1 or len(history) - origins[0] != applied:
        raise ValueError("Approved controller transition lacks recorded history")
    for index, entry in enumerate(history[-applied:]):
        previous, target = approved[index:index + 2]
        if (not isinstance(entry, dict) or entry.get("previous") != previous or
                entry.get("current") != target):
            raise ValueError("Recorded controller history differs from the approved chain")
        changed = entry.get("files")
        if (not isinstance(changed, list) or not changed or
                any(not isinstance(path, str) for path in changed) or
                len(set(changed)) != len(changed)):
            raise ValueError("Recorded controller files are malformed")
        comparison = api.request("GET", f"/compare/{previous}...{target}")
        files = comparison.get("files")
        if (comparison.get("status") != "ahead" or not isinstance(files, list) or
                not files or len(files) >= 300 or
                any(not isinstance(item, dict) or item.get("filename") not in PARALLEL_CONTROLLER_FILES
                    or item.get("status") == "renamed" for item in files)):
            raise ValueError("Controller change modifies protected numerical sources or adapters")
        if set(changed) != {item["filename"] for item in files}:
            raise ValueError("Recorded controller files differ from the verified revision")
    return current


def parallel_predecessor(api, state, sha):
    request = state["requests"][len(state["dispatched"])-1]
    receipt = state["dispatched"][request["key"]]
    first = checked_parallel_run(api, receipt["run_id"])
    if (first.get("head_sha") != request["source_ref"] or
            first.get("display_title") != parallel_title(request["inputs"])):
        return pause(api, state, sha, "Parallel predecessor source changed", "blocked")
    if first.get("status") != "completed":
        return dict(status="waiting", run_id=first["id"], run_status=first.get("status"))
    interrupted = set(request.get("interrupted_runs", []))
    if first.get("conclusion") == "cancelled" and first["id"] not in interrupted:
        return pause(api, state, sha, "Parallel predecessor was cancelled")
    campaign = receipt["campaign_id"]
    saved = parallel_state_document(api, campaign)
    if not isinstance(saved, dict) or saved.get("campaign_id") != campaign:
        return pause(api, state, sha, "Saved parallel campaign differs from the dispatched stage", "blocked")
    try:
        controller = approved_parallel_controller(api, request, saved)
    except ValueError as error:
        return pause(api, state, sha, f"Saved parallel controller is invalid: {error}", "blocked")
    waves = saved.get("waves", [])
    if (not waves or not isinstance(waves, list) or
            any(not isinstance(wave, dict) or type(wave.get("run_id")) is not int for wave in waves) or
            receipt["run_id"] not in {wave["run_id"] for wave in waves}):
        return pause(api, state, sha, "Parallel dispatch is absent from the saved campaign history", "blocked")
    latest = checked_parallel_run(api, waves[-1]["run_id"])
    if latest.get("head_sha") != controller:
        return pause(api, state, sha, "Latest parallel wave uses a different controller", "blocked")
    continuation_title = parallel_title(dict(campaign_id=campaign))
    if latest["id"] != first["id"] and latest.get("display_title") != continuation_title:
        return pause(api, state, sha, "Latest parallel wave belongs to another campaign", "blocked")
    if latest.get("conclusion") == "cancelled":
        return pause(api, state, sha, "Latest parallel wave was cancelled")
    origin = next(index for index, wave in enumerate(waves) if wave["run_id"] == first["id"])
    stage_waves = waves[origin:]
    older_ids = [wave["run_id"] for wave in stage_waves[:-1]]
    if (len({wave["run_id"] for wave in stage_waves}) != len(stage_waves) or
            interrupted - set(older_ids)):
        return pause(api, state, sha, "Interrupted parallel run lacks a newer saved continuation", "blocked")
    approved = request.get("approved_controllers", [request["source_ref"]])
    applied = set(approved[:approved.index(controller) + 1])
    for run_id in older_ids:
        run = first if run_id == first["id"] else checked_parallel_run(api, run_id)
        expected = parallel_title(request["inputs"]) if run_id == first["id"] else continuation_title
        if (run.get("head_sha") not in applied or run.get("display_title") != expected or
                run_id >= latest["id"]):
            return pause(api, state, sha, "Earlier parallel wave differs from the saved campaign", "blocked")
        if run.get("status") != "completed":
            return dict(status="waiting", campaign_id=campaign, run_id=run_id, run_status=run.get("status"))
        if run.get("conclusion") == "cancelled":
            if run_id not in interrupted:
                return pause(api, state, sha, "Earlier parallel wave was cancelled without an approved interruption")
        elif run_id in interrupted:
            return pause(api, state, sha, "Listed interrupted parallel run was not cancelled", "blocked")
    if latest.get("status") != "completed":
        return dict(status="waiting", campaign_id=campaign, run_id=latest["id"], run_status=latest.get("status"))
    status = saved.get("status")
    if status in {"paused", "blocked", "publication_blocked"}:
        return pause(api, state, sha, f"Parallel campaign {campaign} requires review: {status}",
                     "blocked" if status != "paused" else "paused")
    if status in {"active", "publishing"}:
        return dict(status="waiting", campaign_id=campaign, campaign_status=status, run_id=latest["id"])
    if status != "complete":
        return pause(api, state, sha, "Unknown parallel campaign status", "blocked")
    if (saved.get("blocked") or saved.get("publication_pending") or
            not isinstance(saved.get("completed"), list) or latest.get("conclusion") != "success"):
        return pause(api, state, sha, "Parallel campaign completion lacks a successful final publication", "blocked")
    if saved.get("next_platform_campaign"):
        return pause(api, state, sha, "An existing platform continuation conflicts with the ordered sequence", "blocked")
    return None


def advance_parallel(api, state, sha):
    if state.get("dispatch_intent") and state["status"] in {"active", "blocked"}:
        return recover_parallel_intent(api, state, sha)
    if state["status"] != "active":
        return dict(status=state["status"], reason=state.get("reason"))
    if state["dispatched"]:
        waiting = parallel_predecessor(api, state, sha)
        if waiting is not None:
            return waiting
    index = len(state["dispatched"])
    if index == len(state["requests"]):
        state.update(status="complete", updated_at=utc_now())
        save_queue(api, state, sha)
        api.request("PUT", f"/actions/workflows/{QUEUE_WORKFLOW}/disable")
        return dict(status="complete", stages=index)
    request = state["requests"][index]
    scope = "heads" if request["dispatch_ref"] == "main" else "tags"
    ref = api.request("GET", f"/git/ref/{scope}/{quote(request['dispatch_ref'], safe='')}")
    if ref.get("object", {}).get("sha") != request["source_ref"] or ref["object"].get("type") != "commit":
        return pause(api, state, sha, "Parallel dispatch ref differs from the pinned stage", "blocked")
    state["dispatch_intent"] = dict(request, created_at=utc_now())
    state["updated_at"] = utc_now()
    sha = save_queue(api, state, sha)
    result = api.request("POST", "/actions/workflows/parallel-spectra.yml/dispatches",
        dict(ref=request["dispatch_ref"], inputs=request["inputs"], return_run_details=True))
    identity = result.get("workflow_run_id") if isinstance(result, dict) else None
    if type(identity) is not int or identity < 1:
        return pause(api, state, sha, "Parallel workflow returned no verifiable dispatch receipt", "blocked")
    return parallel_receipt(api, state, sha, identity)



def advance(api):
    """Perform at most one dispatch and leave an active benchmark untouched."""
    state, sha = load_queue(api)
    if state["schema_version"] == 2:
        return advance_parallel(api, state, sha)
    if state.get("dispatch_intent") and state["status"] in {"active", "blocked"}:
        return recover_intent(api, state, sha)
    if state["status"] == "complete":
        api.request("PUT", f"/actions/workflows/{QUEUE_WORKFLOW}/disable")
        return dict(status="complete")
    if state["status"] != "active":
        return dict(status=state["status"], reason=state.get("reason"))
    index = len(state["dispatched"])
    previous = (state["dispatched"][state["requests"][index - 1]["key"]]["run_id"]
                if index else state["initial_run_id"])
    run = checked_run(api, previous)
    if run.get("status") != "completed":
        return dict(status="waiting", run_id=previous, run_status=run.get("status"))
    if run.get("conclusion") == "cancelled":
        return pause(api, state, sha, f"Benchmark {previous} was cancelled. The sequence is paused")
    recovering = (index == 0 and state.get("recovery", {}).get("run_id") == previous and
                  run.get("conclusion") == "failure")
    if run.get("conclusion") != "success" and not recovering:
        return pause(api, state, sha,
                     f"Benchmark {previous} ended with {run.get('conclusion')}. Repair it before advancing")
    if not has_saved_baseline(api, previous):
        return pause(api, state, sha, f"Benchmark {previous} has no available timing checkpoint or retained baseline")
    request, inputs, continuation = None, None, {}
    if index and state["requests"][index - 1].get("finish_before_next"):
        pending_request = state["requests"][index - 1]
        receipt = state["dispatched"][pending_request["key"]]
        try:
            recorded, planned = completion_progress(api, run, pending_request,
                receipt.get("source_ref", state["source_ref"]))
        except (ValueError, KeyError) as error:
            return pause(api, state, sha, f"Cannot verify benchmark {previous} completion: {error}")
        if recorded < planned:
            if "recorded_before" in receipt and recorded <= receipt["recorded_before"]:
                return pause(api, state, sha,
                    f"Benchmark {previous} added no saved predictions ({recorded}/{planned}). Inspect it before continuing")
            request = pending_request
            inputs = dict(request["inputs"], mode="extend", refresh_software=request["software"],
                          resume_run_id=str(previous), continuation_platform=request_platform(request))
            continuation = dict(continuation_of=previous, recorded_before=recorded)
    if request is None and index == len(state["requests"]):
        state.update(status="complete", updated_at=utc_now())
        save_queue(api, state, sha)
        api.request("PUT", f"/actions/workflows/{QUEUE_WORKFLOW}/disable")
        return dict(status="complete", runs=index)
    if request is None:
        request = state["requests"][index]
        inputs = resolved_inputs(state, request)
    if request.get("parent") or request.get("baseline_run_id") is not None:
        parent_id = int(inputs["resume_run_id"])
        parent = checked_run(api, parent_id)
        recover_parent = recovering and parent_id == previous
        if (parent.get("status") != "completed" or
                parent.get("conclusion") != "success" and not recover_parent or
                not has_saved_baseline(api, parent_id)):
            return pause(api, state, sha, f"Required same-size parent {parent_id} is not available for continuation")
        if inputs["mode"] == "refresh":
            try:
                saved = saved_checkpoint_settings(api, parent, request_platform(request))
                if saved["configuration"]["geometry"]["semimajor_m"] != float(request["semimajor_m"]):
                    raise ValueError("Saved baseline has a different semimajor axis")
            except (ValueError, KeyError) as error:
                return pause(api, state, sha, f"Cannot verify refresh baseline {parent_id}: {error}")
    ref = state.get("dispatch_ref", "main")
    scope = "heads" if ref == "main" else "tags"
    actual = api.request("GET", f"/git/ref/{scope}/{quote(ref, safe='')}")
    if actual.get("object", {}).get("sha") != state["source_ref"] or actual["object"].get("type") != "commit":
        return pause(api, state, sha, "Dispatch ref no longer matches the saved benchmark commit")
    state["dispatch_intent"] = dict(key=request["key"], inputs=inputs,
                                    source_ref=state["source_ref"], created_at=utc_now(), **continuation)
    state["updated_at"] = utc_now()
    sha = save_queue(api, state, sha)
    # A timeout after this point may mean GitHub accepted the dispatch. The
    # persisted intent is reconciled on the next invocation without a retry.
    result = api.request("POST", "/actions/workflows/benchmark.yml/dispatches",
                         dict(ref=ref, inputs=inputs, return_run_details=True))
    identity = result.get("workflow_run_id") if isinstance(result, dict) else None
    if type(identity) is not int or identity < 1:
        return pause(api, state, sha, "GitHub accepted no verifiable dispatch receipt", "blocked")
    return record_receipt(api, state, sha, identity, inputs)


def advance_when_ready(api, wait_seconds=0):
    """Wait briefly for a closing predecessor, without repeating a dispatch."""
    if type(wait_seconds) is not int or not 0 <= wait_seconds <= 120:
        raise ValueError("wait_seconds must be between 0 and 120")
    deadline = time.monotonic() + wait_seconds
    while True:
        result = advance(api)
        remaining = deadline - time.monotonic()
        if result["status"] != "waiting" or remaining <= 0:
            return result
        time.sleep(min(5, remaining))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"),
                        required=not os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--wait-seconds", type=int, default=0,
                        help="Wait up to 120 seconds for a closing predecessor (default: 0)")
    args = parser.parse_args()
    if not 0 <= args.wait_seconds <= 120:
        parser.error("--wait-seconds must be between 0 and 120")
    api = GitHub(args.repository, os.environ.get("GITHUB_TOKEN"), timeout_seconds=300 + args.wait_seconds)
    result = advance_when_ready(api, args.wait_seconds)
    print(json.dumps(result))


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError, KeyError) as error:
        print(f"Benchmark queue did not advance: {error}", file=sys.stderr)
        raise SystemExit(2)

