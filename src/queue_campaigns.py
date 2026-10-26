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
    platform = inputs.get("continuation_platform") if inputs.get("mode") == "extend" else inputs.get("platform")
    if platform not in PLATFORMS:
        raise ValueError("Finishing a queued software request requires one explicit runner platform")
    return platform


def receipt_run_ids(state):
    return {receipt["run_id"] for current in state["dispatched"].values()
            for receipt in [*current.get("history", []), current]}


def validate_state(state, repository):
    """Reject altered repositories, nonsequential receipts, and unresolved parents."""
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
        if (parent is not None and parent.split(":")[1] != request["semimajor_m"] or
                inputs.get("mode") != ("extend" if parent else "new") or
                inputs.get("refresh_software" if parent else "software") != request["software"] or
                not parent and inputs.get("semimajor_m") != request["semimajor_m"] or
                "resume_run_id" in inputs):
            raise ValueError("Queue inputs do not match the saved software and size")
        if type(request.get("finish_before_next", False)) is not bool:
            raise ValueError("finish_before_next must be a boolean")
        if request.get("finish_before_next"):
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
                        (type(previous["recorded_before"]) is not int or previous["recorded_before"] < 0)):
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


def completion_progress(api, run, request, source_ref):
    """Count immutable saved predictions for the exact requested software and size."""
    artifacts = []
    for page in range(1, 11):
        entries = api.request("GET", f"/actions/runs/{run['id']}/artifacts?per_page=100&page={page}")["artifacts"]
        artifacts.extend(entries)
        if len(entries) < 100:
            break
    else:
        raise ValueError("Too many artifacts to verify completed predictions")
    platform = request_platform(request)
    selected = select_resume_artifacts(artifacts, platform=platform)[platform]
    settings = checkpoint_settings(api.archive(selected["id"]), api.repository, run.get("head_sha", ""), platform)
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
    return inputs


def expected_title(inputs):
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
        url=f"https://github.com/{api.repository}/actions/runs/{identity}", inputs=inputs)
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


def advance(api):
    """Perform at most one dispatch and leave an active benchmark untouched."""
    state, sha = load_queue(api)
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
            recorded, planned = completion_progress(api, run, pending_request, state["source_ref"])
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
    if request.get("parent"):
        parent_id = int(inputs["resume_run_id"])
        parent = checked_run(api, parent_id)
        recover_parent = recovering and parent_id == previous
        if (parent.get("status") != "completed" or
                parent.get("conclusion") != "success" and not recover_parent or
                not has_saved_baseline(api, parent_id)):
            return pause(api, state, sha, f"Required same-size parent {parent_id} is not available for continuation")
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
