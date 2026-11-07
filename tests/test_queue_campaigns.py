"""Durable benchmark sequencing without resubmitting uncertain dispatches."""

import base64
import copy
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import queue_campaigns as queue
import benchmark


def request(program, size, parent):
    inputs = dict(mode="extend", refresh_software=program) if parent else dict(
        mode="new", software=program, semimajor_m=size, platform="all")
    inputs.update(timing_budget_minutes="240", reference_budget_minutes="0")
    return dict(key=f"{program}:{size}", software=program, semimajor_m=size, parent=parent, inputs=inputs)


def state():
    return dict(schema_version=1, repository="owner/repo", status="active", source_ref="a" * 40,
        dispatch_ref="benchmark-series-20261025", initial_run_id=100, dispatched={}, requests=[
            request("prol_spheroid", "0.01", "acousticts:0.01"),
            request("echosms", "0.01", "prol_spheroid:0.01"),
            request("acousticts", "0.001", None),
            request("acousticts", "0.07", None),
            request("prol_spheroid", "0.001", "acousticts:0.001")])


def timing_checkpoint(recorded=1, software="prol_spheroid", semimajor=.01, source_ref="a" * 40,
                      platform="Linux", alter=None):
    configuration = dict(schema_version=1, geometry=dict(semimajor_m=semimajor, aspect_ratios=[10]),
        incidences_deg=[90], boundaries=["gas"], software=[software], repetitions=1, warmup=True,
        frequency=dict(start_hz=1000, stop_hz=3000, step_hz=1000),
        medium=dict(density_kg_m3=1026.8, sound_speed_m_s=1477.3),
        materials=dict(gas=dict(density_kg_m3=1.24, sound_speed_m_s=345)),
        controls={name: {} for name in benchmark.CAPABILITIES}, profiles=dict(pilot={}))
    cases = benchmark.build_cases(configuration)
    identity = dict(configuration=configuration, sources=dict(sources={software: dict(sha="a" * 40)}),
        profile="pilot", cases=[case["case_id"] for case in cases], adapter_hashes={},
        platform=platform, architecture="x86_64", environment_fingerprint="b" * 64,
        workload="fresh_process_warmed_distinct_problem_single_prediction")
    records = {case["case_id"]: dict(case_id=case["case_id"], status="ok", elapsed_seconds=.2,
        ts_db=-40, source_sha="a" * 40) for case in cases[:recorded]}
    files = {"manifest.json": dict(identity, fingerprint=benchmark.fingerprint(identity)),
             "status.json": dict(state="budget_paused" if recorded < len(cases) else "complete",
                                 planned=len(cases), recorded=recorded),
             "results.json": benchmark.summarize(cases, records, {}),
             "benchmark-source.json": dict(schema_version=1, repository="owner/repo", source_ref=source_ref),
             **{f"records/{key}.json": value for key, value in records.items()}}
    if alter:
        alter(files)
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, value in files.items():
            archive.writestr(name, json.dumps(value))
    return output.getvalue()


class FakeAPI:
    repository = "owner/repo"

    def __init__(self, saved=None):
        self.state = copy.deepcopy(saved or state())
        self.calls = []
        self.runs = {100: self.run(100)}
        self.artifacts = {100: [dict(name="results-ubuntu-latest", expired=False)]}
        self.next_run = 101
        self.dispatch_error = None
        self.accept_before_error = False
        self.empty_receipt = False
        self.ref_sha = "a" * 40
        self.disabled = False
        self.archives = {}
        self.downloads = []

    def run(self, identity, inputs=None):
        return dict(id=identity, status="completed", conclusion="success", event="workflow_dispatch",
            path=queue.BENCHMARK_WORKFLOW, repository=dict(full_name=self.repository),
            head_repository=dict(full_name=self.repository), head_sha="a" * 40,
            created_at=datetime.now(timezone.utc).isoformat(),
            display_title=queue.expected_title(inputs) if inputs else "Original benchmark")

    def sha(self):
        return hashlib.sha256(json.dumps(self.state, sort_keys=True).encode()).hexdigest()

    def request(self, method, path, body=None):
        self.calls.append((method, path, copy.deepcopy(body)))
        if path == f"/contents/{queue.QUEUE_FILE}?ref=benchmark-data":
            return dict(sha=self.sha(), encoding="base64", content=base64.b64encode(json.dumps(self.state).encode()).decode())
        if method == "PUT" and path == f"/contents/{queue.QUEUE_FILE}":
            if body["sha"] != self.sha():
                raise queue.APIError(409, "Content conflict")
            self.state = json.loads(base64.b64decode(body["content"]))
            return dict(content=dict(sha=self.sha()))
        if path.startswith("/git/ref/"):
            return dict(object=dict(type="commit", sha=self.ref_sha))
        if path == "/actions/workflows/benchmark.yml/dispatches":
            if self.dispatch_error and not self.accept_before_error:
                raise self.dispatch_error
            identity = self.next_run
            self.next_run += 1
            self.runs[identity] = dict(self.run(identity, body["inputs"]), status="pending", conclusion=None,
                                       head_sha=self.ref_sha)
            if self.dispatch_error:
                raise self.dispatch_error
            return {} if self.empty_receipt else dict(workflow_run_id=identity)
        if path.startswith("/actions/workflows/benchmark.yml/runs?"):
            return dict(workflow_runs=list(self.runs.values()))
        if path == f"/actions/workflows/{queue.QUEUE_WORKFLOW}/disable":
            self.disabled = True
            return None
        if path.startswith("/actions/runs/"):
            identity = int(path.split("/")[3])
            if "/artifacts?" in path:
                return dict(artifacts=self.artifacts.get(identity, []))
            return copy.deepcopy(self.runs[identity])
        raise AssertionError((method, path, body))

    def finish(self, identity, conclusion="success", artifact="results-ubuntu-latest"):
        self.runs[identity].update(status="completed", conclusion=conclusion)
        self.artifacts[identity] = [dict(name=artifact, expired=False)] if artifact else []

    def finish_predictions(self, identity, recorded=1, **kwargs):
        self.finish(identity)
        self.artifacts[identity][0]["id"] = identity * 100
        self.archives[identity * 100] = timing_checkpoint(recorded=recorded, **kwargs)

    def archive(self, identity):
        self.downloads.append(identity)
        return self.archives[identity]

    def dispatches(self):
        return [body for method, path, body in self.calls if method == "POST" and path.endswith("/dispatches")]


class QueueTests(unittest.TestCase):
    def completing_api(self):
        saved = state()
        saved["requests"][0]["finish_before_next"] = True
        saved["requests"][0]["inputs"]["continuation_platform"] = "ubuntu-latest"
        return FakeAPI(saved)

    def test_complete_sequence_resolves_same_size_parent_and_disables_controller(self):
        api = FakeAPI()
        receipts = []
        for item in api.state["requests"]:
            result = queue.advance(api)
            self.assertEqual(result["status"], "dispatched")
            self.assertEqual(result["key"], item["key"])
            receipts.append(result["run_id"])
            api.finish(result["run_id"])
        self.assertEqual(receipts, [101, 102, 103, 104, 105])
        self.assertEqual(api.dispatches()[-1]["inputs"]["resume_run_id"], "103")
        self.assertNotEqual(api.dispatches()[-1]["inputs"]["resume_run_id"], "104")
        self.assertTrue(all(body["return_run_details"] for body in api.dispatches()))
        self.assertTrue(all(body["ref"] == "benchmark-series-20261025" for body in api.dispatches()))
        self.assertEqual(queue.advance(api)["status"], "complete")
        self.assertTrue(api.disabled)
        self.assertEqual(api.state["status"], "complete")
        queue.advance(api)
        self.assertEqual(len(api.dispatches()), 5)

    def test_active_predecessor_is_untouched_and_duplicate_events_do_not_dispatch(self):
        api = FakeAPI()
        api.runs[100]["status"] = "in_progress"
        self.assertEqual(queue.advance(api)["status"], "waiting")
        self.assertFalse(api.dispatches())
        self.assertFalse(any(method != "GET" for method, _, _ in api.calls))
        api.finish(100)
        queue.advance(api)
        queue.advance(api)
        queue.advance(api)
        self.assertEqual(len(api.dispatches()), 1)

    def test_explicit_cancellation_pauses_even_with_saved_results(self):
        api = FakeAPI()
        api.finish(100, "cancelled")
        self.assertEqual(queue.advance(api)["status"], "paused")
        self.assertIn("cancelled", api.state["reason"])
        queue.advance(api)
        self.assertFalse(api.dispatches())

    def test_unsuccessful_run_pauses_even_with_retained_baseline(self):
        for conclusion in ("failure", "timed_out", "action_required", "startup_failure", None):
            with self.subTest(conclusion=conclusion):
                api = FakeAPI()
                api.finish(100, conclusion, "retained-windows-latest")
                self.assertEqual(queue.advance(api)["status"], "paused")
                self.assertFalse(api.dispatches())
                self.assertIn("Repair it", api.state["reason"])

    def test_no_baseline_or_expired_results_pause(self):
        for entries in ([], [dict(name="environment-ubuntu-latest")],
                        [dict(name="results-ubuntu-latest", expired=True)]):
            with self.subTest(entries=entries):
                api = FakeAPI()
                api.artifacts[100] = entries
                self.assertEqual(queue.advance(api)["status"], "paused")
                self.assertFalse(api.dispatches())

    def test_explicit_recovery_waits_for_active_run_and_retains_its_baseline(self):
        saved = state()
        saved.update(initial_key="echosms_baseline:0.01",
                     recovery=dict(run_id=100, reason="Fixed compiler selection and retained metadata size"))
        saved["requests"][0]["parent"] = saved["initial_key"]
        api = FakeAPI(saved)
        api.runs[100].update(status="in_progress", conclusion=None)
        self.assertEqual(queue.advance(api)["status"], "waiting")
        self.assertFalse(api.dispatches())
        api.finish(100, "failure")
        self.assertEqual(queue.advance(api)["status"], "dispatched")
        self.assertEqual(api.dispatches()[0]["inputs"]["resume_run_id"], "100")
        # An explicit repair never authorizes ignoring a subsequent failure.
        api.finish(101, "failure")
        self.assertEqual(queue.advance(api)["status"], "paused")
        self.assertEqual(len(api.dispatches()), 1)

    def test_recovery_cannot_bypass_cancellation_or_missing_artifacts(self):
        for conclusion, artifact in (("cancelled", "results-ubuntu-latest"), ("failure", None)):
            api = FakeAPI()
            api.state["recovery"] = dict(run_id=100, reason="Explicit repair")
            api.finish(100, conclusion, artifact)
            self.assertEqual(queue.advance(api)["status"], "paused")
            self.assertFalse(api.dispatches())

    def test_dispatch_intent_is_saved_before_post_and_timeout_never_retried(self):
        api = FakeAPI()
        api.dispatch_error = TimeoutError("Network timed out")
        with self.assertRaises(TimeoutError):
            queue.advance(api)
        self.assertEqual(api.state["dispatch_intent"]["key"], "prol_spheroid:0.01")
        writes = [index for index, call in enumerate(api.calls) if call[0] == "PUT"]
        post = next(index for index, call in enumerate(api.calls) if call[0] == "POST")
        self.assertLess(writes[0], post)
        self.assertEqual(queue.advance(api)["status"], "blocked")
        self.assertEqual(queue.advance(api)["status"], "blocked")
        self.assertEqual(len(api.dispatches()), 1)

    def test_accepted_dispatch_with_lost_response_is_adopted_without_duplicate(self):
        api = FakeAPI()
        api.dispatch_error = TimeoutError("Response lost")
        api.accept_before_error = True
        with self.assertRaises(TimeoutError):
            queue.advance(api)
        result = queue.advance(api)
        self.assertEqual(result["run_id"], 101)
        self.assertEqual(api.state["dispatched"]["prol_spheroid:0.01"]["run_id"], 101)
        self.assertNotIn("dispatch_intent", api.state)
        self.assertEqual(len(api.dispatches()), 1)
        self.assertEqual(queue.advance(api)["status"], "waiting")

    def test_delayed_dispatch_metadata_retries_only_reads(self):
        api = FakeAPI()
        original = api.request
        reads = []
        def delayed(method, path, body=None):
            result = original(method, path, body)
            if method == "GET" and path == "/actions/runs/101":
                reads.append(path)
                if len(reads) < 3:
                    result["display_title"] = "Dense spheroid timings"
            return result
        api.request = delayed
        with patch.object(queue.time, "sleep"):
            self.assertEqual(queue.advance(api)["status"], "dispatched")
        self.assertEqual(len(reads), 3)
        self.assertEqual(len(api.dispatches()), 1)

    def test_persistent_wrong_dispatch_metadata_blocks_without_redispatch(self):
        api = FakeAPI()
        original = api.request
        def wrong(method, path, body=None):
            result = original(method, path, body)
            if method == "GET" and path == "/actions/runs/101":
                result["head_sha"] = "b" * 40
            return result
        api.request = wrong
        with patch.object(queue.time, "sleep"):
            self.assertEqual(queue.advance(api)["status"], "blocked")
        self.assertEqual(len(api.dispatches()), 1)
        self.assertIn("dispatch_intent", api.state)

    def test_empty_dispatch_receipt_blocks_then_recovers_existing_run(self):
        api = FakeAPI()
        api.empty_receipt = True
        self.assertEqual(queue.advance(api)["status"], "blocked")
        self.assertEqual(queue.advance(api)["run_id"], 101)
        self.assertEqual(len(api.dispatches()), 1)

    def test_ambiguous_recovery_does_not_guess_or_dispatch_again(self):
        api = FakeAPI()
        api.empty_receipt = True
        queue.advance(api)
        api.runs[102] = dict(api.runs[101], id=102)
        self.assertEqual(queue.advance(api)["status"], "blocked")
        self.assertEqual(len(api.dispatches()), 1)

    def test_changed_dispatch_tag_pauses(self):
        api = FakeAPI()
        api.ref_sha = "b" * 40
        self.assertEqual(queue.advance(api)["status"], "paused")
        self.assertFalse(api.dispatches())

    def test_repository_and_workflow_must_match(self):
        for change in (dict(head_repository=dict(full_name="fork/repo")),
                       dict(path=".github/workflows/other.yml"), dict(event="push")):
            with self.subTest(change=change):
                api = FakeAPI()
                api.runs[100].update(change)
                with self.assertRaises(ValueError):
                    queue.advance(api)
                self.assertFalse(api.dispatches())

    def test_missing_or_future_parent_and_nonprefix_receipts_are_rejected(self):
        for mutate in (
                lambda saved: saved["requests"][0].update(parent="echosms:0.01"),
                lambda saved: saved["requests"][0].update(parent="acousticts:0.001"),
                lambda saved: saved["dispatched"].update({"echosms:0.01": dict(run_id=102, inputs={})})):
            api = FakeAPI()
            mutate(api.state)
            with self.assertRaises(ValueError):
                queue.advance(api)
            self.assertFalse(api.dispatches())

    def test_concurrent_queue_edit_stops_before_dispatch(self):
        api = FakeAPI()
        original = api.request
        def conflict(method, path, body=None):
            if method == "PUT" and path == f"/contents/{queue.QUEUE_FILE}":
                api.state["status"] = "paused"
                raise queue.APIError(409, "Conflict")
            return original(method, path, body)
        api.request = conflict
        with self.assertRaisesRegex(RuntimeError, "changed concurrently"):
            queue.advance(api)
        self.assertFalse(api.dispatches())

    def test_finish_gate_extends_same_software_until_complete_and_preserves_receipt_history(self):
        api = self.completing_api()
        self.assertEqual(queue.advance(api)["run_id"], 101)
        for previous, recorded in ((101, 1), (102, 2)):
            api.finish_predictions(previous, recorded)
            result = queue.advance(api)
            self.assertEqual(result["key"], "prol_spheroid:0.01")
            self.assertEqual(result["run_id"], previous + 1)
            receipt = api.state["dispatched"][result["key"]]
            self.assertEqual(receipt["recorded_before"], recorded)
            self.assertEqual([item["run_id"] for item in receipt["history"]], list(range(101, previous + 1)))
            self.assertTrue(all("history" not in item for item in receipt["history"]))
            self.assertEqual(receipt["inputs"]["resume_run_id"], str(previous))
            self.assertEqual(receipt["inputs"]["continuation_platform"], "ubuntu-latest")
            self.assertEqual(receipt["inputs"]["refresh_software"], "prol_spheroid")
            self.assertNotIn("finish_before_next", receipt["inputs"])
            self.assertEqual(queue.advance(api)["status"], "waiting")
        api.finish_predictions(103, 3)
        result = queue.advance(api)
        self.assertEqual(result["key"], "echosms:0.01")
        self.assertEqual(api.dispatches()[-1]["inputs"]["resume_run_id"], "103")
        self.assertEqual(len(api.dispatches()), 4)

    def test_last_request_is_not_complete_until_all_predictions_are_saved(self):
        api = self.completing_api()
        api.state["requests"] = api.state["requests"][:1]
        queue.advance(api)
        api.finish_predictions(101, 1)
        self.assertEqual(queue.advance(api)["status"], "dispatched")
        self.assertFalse(api.disabled)
        api.finish_predictions(102, 3)
        self.assertEqual(queue.advance(api)["status"], "complete")
        self.assertTrue(api.disabled)
        self.assertEqual(len(api.dispatches()), 2)

    def test_controller_update_preserves_original_checkpoint_revision(self):
        api = self.completing_api()
        queue.advance(api)
        api.finish_predictions(101, 1)
        # A publication-only update must not relabel the earlier measurements.
        api.state.update(source_ref="b" * 40, dispatch_ref="updated-progress-controller")
        api.ref_sha = "b" * 40
        result = queue.advance(api)
        self.assertEqual(result["status"], "dispatched")
        receipt = api.state["dispatched"]["prol_spheroid:0.01"]
        self.assertEqual(receipt["source_ref"], "b" * 40)
        self.assertEqual(receipt["history"][0]["source_ref"], "a" * 40)
        self.assertEqual(receipt["recorded_before"], 1)
        self.assertEqual(api.dispatches()[-1]["inputs"]["resume_run_id"], "101")
        api.finish_predictions(102, 3, source_ref="b" * 40)
        self.assertEqual(queue.advance(api)["key"], "echosms:0.01")

    def test_no_progress_or_lost_records_pause_instead_of_repeating_empty_runs(self):
        for first, second in ((0, 0), (1, 1), (2, 1)):
            with self.subTest(first=first, second=second):
                api = self.completing_api()
                queue.advance(api)
                api.finish_predictions(101, first)
                queue.advance(api)
                api.finish_predictions(102, second)
                result = queue.advance(api)
                self.assertEqual(result["status"], "paused")
                self.assertIn("no saved predictions", result["reason"])
                self.assertEqual(queue.advance(api)["status"], "paused")
                self.assertEqual(len(api.dispatches()), 2)

    def test_completion_checks_actual_checkpoint_inputs_sources_platform_and_records(self):
        for kwargs in (dict(software="echosms"), dict(semimajor=.07), dict(source_ref="b" * 40),
                       dict(platform="Windows"), dict(alter=lambda files: files["status.json"].update(recorded=3))):
            with self.subTest(kwargs=kwargs):
                api = self.completing_api()
                queue.advance(api)
                api.finish_predictions(101, **kwargs)
                result = queue.advance(api)
                self.assertEqual(result["status"], "paused")
                self.assertIn("Cannot verify", result["reason"])
                self.assertEqual(len(api.dispatches()), 1)

    def test_completion_downloads_only_selected_platform_and_requires_its_checkpoint(self):
        api = self.completing_api()
        queue.advance(api)
        api.finish_predictions(101)
        api.artifacts[101].append(dict(id=999, name="results-windows-latest", expired=False))
        api.archives[999] = b"corrupt unrelated checkpoint"
        self.assertEqual(queue.advance(api)["status"], "dispatched")
        self.assertEqual(api.downloads, [10100])
        api.finish(102, artifact="results-windows-latest")
        api.artifacts[102][0]["id"] = 999
        self.assertEqual(queue.advance(api)["status"], "paused")
        self.assertEqual(api.downloads, [10100])

    def test_failed_or_cancelled_continuation_never_retries_automatically(self):
        for conclusion in ("failure", "cancelled", "timed_out"):
            with self.subTest(conclusion=conclusion):
                api = self.completing_api()
                queue.advance(api)
                api.finish_predictions(101)
                queue.advance(api)
                api.finish(102, conclusion)
                self.assertEqual(queue.advance(api)["status"], "paused")
                self.assertEqual(len(api.dispatches()), 2)

    def test_lost_continuation_receipt_recovers_history_without_second_post(self):
        api = self.completing_api()
        queue.advance(api)
        original = copy.deepcopy(api.state["dispatched"]["prol_spheroid:0.01"])
        api.finish_predictions(101, 1)
        api.dispatch_error = TimeoutError("Accepted response lost")
        api.accept_before_error = True
        with self.assertRaises(TimeoutError):
            queue.advance(api)
        self.assertEqual(api.state["dispatch_intent"]["continuation_of"], 101)
        self.assertEqual(api.state["dispatch_intent"]["recorded_before"], 1)
        self.assertEqual(api.state["dispatched"]["prol_spheroid:0.01"], original)
        self.assertEqual(queue.advance(api)["run_id"], 102)
        receipt = api.state["dispatched"]["prol_spheroid:0.01"]
        self.assertEqual(receipt["history"], [original])
        self.assertNotIn("dispatch_intent", api.state)
        self.assertEqual(queue.advance(api)["status"], "waiting")
        self.assertEqual(len(api.dispatches()), 2)

    def test_completion_gate_and_continuation_history_metadata_are_validated(self):
        for value in ("true", 1, None):
            api = self.completing_api()
            api.state["requests"][0]["finish_before_next"] = value
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "boolean"):
                queue.advance(api)
        api = self.completing_api()
        api.state["requests"][0]["inputs"]["continuation_platform"] = "Keep saved platforms"
        with self.assertRaisesRegex(ValueError, "explicit runner"):
            queue.advance(api)
        api = self.completing_api()
        queue.advance(api)
        api.finish_predictions(101)
        queue.advance(api)
        api.state["dispatched"]["prol_spheroid:0.01"]["history"][0]["run_id"] = 102
        with self.assertRaisesRegex(ValueError, "duplicate"):
            queue.advance(api)

    def test_short_wait_retries_only_active_predecessor_and_never_a_dispatch(self):
        api = FakeAPI()
        waiting = dict(status="waiting", run_id=100)
        dispatched = dict(status="dispatched", run_id=101)
        with patch.object(queue, "advance", side_effect=[waiting, waiting, dispatched]) as advance, \
                patch.object(queue.time, "monotonic", side_effect=[0, 0, 5, 10]), \
                patch.object(queue.time, "sleep") as sleep:
            self.assertEqual(queue.advance_when_ready(api, 10), dispatched)
            self.assertEqual(advance.call_count, 3)
            self.assertEqual([call.args[0] for call in sleep.call_args_list], [5, 5])
        for status in ("dispatched", "paused", "blocked", "complete"):
            with self.subTest(status=status), patch.object(queue, "advance", return_value=dict(status=status)) as advance, \
                    patch.object(queue.time, "sleep") as sleep:
                self.assertEqual(queue.advance_when_ready(api, 120)["status"], status)
                advance.assert_called_once_with(api)
                sleep.assert_not_called()

    def test_wait_has_bounded_duration_and_default_never_sleeps(self):
        api = FakeAPI()
        waiting = dict(status="waiting", run_id=100)
        with patch.object(queue, "advance", return_value=waiting) as advance, \
                patch.object(queue.time, "monotonic", side_effect=[0, 0, 4, 6]), \
                patch.object(queue.time, "sleep") as sleep:
            self.assertEqual(queue.advance_when_ready(api, 6), waiting)
            self.assertEqual(advance.call_count, 3)
            self.assertEqual([call.args[0] for call in sleep.call_args_list], [5, 2])
        api.runs[100]["status"] = "in_progress"
        with patch.object(queue.time, "sleep") as sleep:
            self.assertEqual(queue.advance_when_ready(api)["status"], "waiting")
            sleep.assert_not_called()
        for value in (-1, 121, "10", True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                queue.advance_when_ready(api, value)

    def test_cli_network_budget_reserves_checkpoint_verification_after_waiting(self):
        for wait_seconds in (0, 120):
            arguments = ["queue_campaigns.py", "--repository", "owner/repo", "--wait-seconds", str(wait_seconds)]
            with self.subTest(wait_seconds=wait_seconds), patch.object(sys, "argv", arguments), \
                    patch.dict(os.environ, GITHUB_TOKEN="fixture-token"), \
                    patch.object(queue, "GitHub") as client, \
                    patch.object(queue, "advance_when_ready", return_value=dict(status="waiting")) as advance, \
                    patch("builtins.print"):
                queue.main()
                client.assert_called_once_with("owner/repo", "fixture-token", timeout_seconds=300 + wait_seconds)
                advance.assert_called_once_with(client.return_value, wait_seconds)


class SavedRefreshQueueTests(unittest.TestCase):
    def refresh_api(self):
        saved = state()
        saved.update(initial_key="prol_spheroid:0.1", requests=[])
        sizes = ["0.001", "0.01", "0.07", "0.1"]
        for index, size in enumerate(sizes):
            saved["requests"].append(dict(key=f"acousticts:{size}", software="acousticts",
                semimajor_m=size, parent=None, baseline_run_id=200 + index, finish_before_next=True,
                inputs=dict(mode="refresh", refresh_software="acousticts",
                    continuation_platform="ubuntu-latest", timing_budget_minutes="240",
                    reference_budget_minutes="0", boundaries="rigid,pressure_release,liquid,gas")))
        for software, parent in (("echosms", "acousticts"), ("liquid_spheroid", "echosms")):
            for size in sizes:
                item = request(software, size, f"{parent}:{size}")
                item.update(finish_before_next=True)
                item["inputs"]["continuation_platform"] = "ubuntu-latest"
                if software == "liquid_spheroid":
                    item["inputs"].update(continuation_frequency="Use frequency inputs above",
                        frequency_start_khz="1", frequency_end_khz="100", frequency_interval_khz="2")
                saved["requests"].append(item)
        api = FakeAPI(saved)
        for index, size in enumerate(sizes):
            identity = 200 + index
            api.runs[identity] = api.run(identity)
            api.finish_predictions(identity, 3, software="prol_spheroid", semimajor=float(size))
        return api

    def test_twelve_requests_refresh_four_sizes_then_extend_each_software(self):
        api = self.refresh_api()
        queue.validate_state(api.state, api.repository)
        keys = [item["key"] for item in api.state["requests"]]
        for index, item in enumerate(api.state["requests"]):
            result = queue.advance(api)
            self.assertEqual((result["status"], result["key"]), ("dispatched", keys[index]))
            dispatched = api.dispatches()[-1]["inputs"]
            if index < 4:
                self.assertEqual(dispatched["mode"], "refresh")
                self.assertEqual(dispatched["resume_run_id"], str(200 + index))
                self.assertNotIn("baseline_run_id", dispatched)
            else:
                expected = api.state["dispatched"][item["parent"]]["run_id"]
                self.assertEqual(dispatched["resume_run_id"], str(expected))
                self.assertEqual(dispatched["mode"], "extend")
            self.assertEqual(queue.advance(api)["status"], "waiting")
            api.finish_predictions(result["run_id"], 3, software=item["software"],
                                   semimajor=float(item["semimajor_m"]))
        self.assertEqual(queue.advance(api)["status"], "complete")
        self.assertTrue(api.disabled)
        self.assertEqual(len(api.dispatches()), 12)

    def test_first_refresh_continues_as_extend_before_next_axis(self):
        api = self.refresh_api()
        first = queue.advance(api)["run_id"]
        api.finish_predictions(first, 1, software="acousticts", semimajor=.001)
        second = queue.advance(api)
        self.assertEqual(second["key"], "acousticts:0.001")
        self.assertEqual(api.dispatches()[-1]["inputs"]["mode"], "extend")
        self.assertEqual(api.dispatches()[-1]["inputs"]["resume_run_id"], str(first))
        self.assertEqual(api.dispatches()[-1]["inputs"]["boundaries"], "rigid,pressure_release,liquid,gas")
        api.finish_predictions(second["run_id"], 3, software="acousticts", semimajor=.001)
        self.assertEqual(queue.advance(api)["key"], "acousticts:0.01")
        self.assertEqual(api.dispatches()[-1]["inputs"]["resume_run_id"], "201")
        receipt = api.state["dispatched"]["acousticts:0.001"]
        self.assertEqual(receipt["history"][0]["inputs"]["mode"], "refresh")
        self.assertEqual(receipt["inputs"]["mode"], "extend")

    def test_refresh_baseline_metadata_is_strict(self):
        changes = [dict(baseline_run_id=value) for value in (None, 0, -1, True, 1.5, "200")]
        changes += [dict(parent="prol_spheroid:0.1"), dict(baseline_run_id=200, parent="prol_spheroid:0.1")]
        for change in changes:
            with self.subTest(change=change):
                api = self.refresh_api()
                api.state["requests"][0].update(change)
                with self.assertRaises(ValueError):
                    queue.validate_state(api.state, api.repository)
        for change in (dict(mode="new"), dict(mode="extend"), dict(refresh_software="echosms"),
                       dict(resume_run_id="200"), dict(continuation_platform="Keep saved platforms")):
            with self.subTest(inputs=change):
                api = self.refresh_api()
                api.state["requests"][0]["inputs"].update(change)
                with self.assertRaises(ValueError):
                    queue.validate_state(api.state, api.repository)

    def test_refresh_requires_finished_available_same_size_baseline(self):
        for mutation in ("running", "failed", "missing", "expired", "wrong_size", "wrong_repository"):
            with self.subTest(mutation=mutation):
                api = self.refresh_api()
                if mutation == "running":
                    api.runs[200]["status"] = "in_progress"
                elif mutation == "failed":
                    api.runs[200]["conclusion"] = "failure"
                elif mutation == "missing":
                    api.artifacts[200] = []
                elif mutation == "expired":
                    api.artifacts[200][0]["expired"] = True
                elif mutation == "wrong_size":
                    api.finish_predictions(200, 3, semimajor=.07)
                else:
                    api.runs[200]["head_repository"]["full_name"] = "other/repo"
                if mutation == "wrong_repository":
                    with self.assertRaises(ValueError):
                        queue.advance(api)
                else:
                    self.assertEqual(queue.advance(api)["status"], "paused")
                self.assertFalse(api.dispatches())

    def test_refresh_dispatch_timeout_is_reconciled_without_reposting(self):
        api = self.refresh_api()
        api.dispatch_error = TimeoutError("Accepted dispatch response lost")
        api.accept_before_error = True
        with self.assertRaises(TimeoutError):
            queue.advance(api)
        self.assertEqual(api.state["dispatch_intent"]["inputs"]["mode"], "refresh")
        self.assertEqual(api.state["dispatch_intent"]["inputs"]["resume_run_id"], "200")
        self.assertEqual(queue.advance(api)["run_id"], 101)
        self.assertEqual(len(api.dispatches()), 1)

    def test_refresh_intent_cannot_change_its_baseline_or_selected_software(self):
        for change in (dict(resume_run_id="201"), dict(refresh_software="echosms"), dict(mode="extend")):
            with self.subTest(change=change):
                api = self.refresh_api()
                api.dispatch_error = TimeoutError("No response")
                with self.assertRaises(TimeoutError):
                    queue.advance(api)
                api.state["dispatch_intent"]["inputs"].update(change)
                with self.assertRaisesRegex(ValueError, "Pending dispatch inputs"):
                    queue.validate_state(api.state, api.repository)

    def test_refresh_never_advances_while_the_previous_request_runs(self):
        api = self.refresh_api()
        result = queue.advance(api)
        self.assertEqual(queue.advance(api)["status"], "waiting")
        self.assertEqual(len(api.dispatches()), 1)
        api.finish_predictions(result["run_id"], 3, software="acousticts", semimajor=.001)
        api.ref_sha = "b" * 40
        self.assertEqual(queue.advance(api)["status"], "paused")
        self.assertEqual(len(api.dispatches()), 1)




def parallel_state():
    first = dict(key="echo-existing", source_ref="a" * 40, dispatch_ref="linux-pinned",
        inputs=dict(mode="repeat", software="echosms", semimajor_axes="0.01",
            additional_repetitions="4", platform="ubuntu-latest", max_parallel="16",
            automatic_continue="true"))
    second = dict(key="acousticts-macos", source_ref="b" * 40, dispatch_ref="platform-pinned",
        inputs=dict(campaign_id="77", platform="macos-latest", max_parallel="16"))
    return dict(schema_version=2, repository="owner/repo", status="active",
                requests=[first, second], dispatched={})


class FakeParallelAPI(FakeAPI):
    def __init__(self, saved=None):
        super().__init__(saved or parallel_state())
        self.campaigns = {}
        self.comparisons = {}

    def run(self, identity, inputs=None):
        return dict(super().run(identity), path=queue.PARALLEL_WORKFLOW,
            display_title=queue.parallel_title(inputs) if inputs else "Original spectra")

    def request(self, method, path, body=None):
        if path.startswith("/compare/"):
            self.calls.append((method, path, copy.deepcopy(body)))
            return copy.deepcopy(self.comparisons[path])
        if path == "/actions/workflows/parallel-spectra.yml/dispatches":
            self.calls.append((method, path, copy.deepcopy(body)))
            if self.dispatch_error and not self.accept_before_error:
                raise self.dispatch_error
            identity = self.next_run
            self.next_run += 1
            self.runs[identity] = dict(self.run(identity, body["inputs"]), status="queued",
                                      conclusion=None, head_sha=self.ref_sha)
            if self.dispatch_error:
                raise self.dispatch_error
            return {} if self.empty_receipt else dict(workflow_run_id=identity)
        if path.startswith("/actions/workflows/parallel-spectra.yml/runs?"):
            self.calls.append((method, path, copy.deepcopy(body)))
            return dict(workflow_runs=list(self.runs.values()))
        return super().request(method, path, body)

    def finish_campaign(self, key, status="complete"):
        receipt = self.state["dispatched"][key]
        self.finish(receipt["run_id"])
        self.campaigns[receipt["campaign_id"]] = dict(campaign_id=receipt["campaign_id"],
            status=status, completed=["task"], blocked=[], waves=[dict(run_id=receipt["run_id"])],
            controller_ref=receipt["source_ref"])
        return self.campaigns[receipt["campaign_id"]]


class ParallelQueueTests(unittest.TestCase):
    def advance(self, api):
        with patch.object(queue, "parallel_state_document",
                          side_effect=lambda client, campaign: copy.deepcopy(client.campaigns.get(campaign))):
            return queue.advance(api)

    def ready(self):
        api = FakeParallelAPI()
        self.assertEqual(self.advance(api)["status"], "dispatched")
        saved = api.finish_campaign("echo-existing")
        return api, saved

    def test_new_then_resumed_campaign_preserve_independent_pins(self):
        api, saved = self.ready()
        self.assertEqual(api.state["dispatched"]["echo-existing"]["campaign_id"], "101")
        api.ref_sha = "b" * 40
        self.assertEqual(self.advance(api)["campaign_id"], "77")
        self.assertEqual(api.state["dispatched"]["acousticts-macos"]["source_ref"], "b" * 40)
        self.assertEqual(api.dispatches()[1]["inputs"], dict(campaign_id="77", platform="macos-latest", max_parallel="16"))
        self.assertEqual([item["ref"] for item in api.dispatches()], ["linux-pinned", "platform-pinned"])
        api.finish_campaign("acousticts-macos")
        self.assertEqual(self.advance(api)["status"], "complete")
        self.assertTrue(api.disabled)
        self.advance(api)
        self.assertEqual(len(api.dispatches()), 2)

    def test_active_run_and_duplicate_events_never_dispatch(self):
        api = FakeParallelAPI()
        self.advance(api)
        for _ in range(3):
            self.assertEqual(self.advance(api)["status"], "waiting")
        self.assertEqual(len(api.dispatches()), 1)

    def test_saved_active_and_publishing_preserve_automatic_continuation(self):
        for status in ("active", "publishing"):
            api, saved = self.ready()
            saved["status"] = status
            for _ in range(2):
                self.assertEqual(self.advance(api)["status"], "waiting")
            self.assertEqual(len(api.dispatches()), 1)

    def test_completion_waits_for_latest_continuation_to_close(self):
        api, saved = self.ready()
        api.runs[103] = dict(api.run(103, dict(campaign_id="101")), status="in_progress", conclusion=None)
        saved["waves"].append(dict(run_id=103))
        self.assertEqual(self.advance(api)["status"], "waiting")
        self.assertEqual(len(api.dispatches()), 1)
        api.finish(103)
        api.ref_sha = "b" * 40
        self.assertEqual(self.advance(api)["status"], "dispatched")

    def test_saved_review_states_halt_without_continuation(self):
        for status in ("paused", "blocked", "publication_blocked"):
            api, saved = self.ready()
            saved["status"] = status
            expected = "paused" if status == "paused" else "blocked"
            self.assertEqual(self.advance(api)["status"], expected)
            self.assertEqual(self.advance(api)["status"], expected)
            self.assertEqual(len(api.dispatches()), 1)

    def test_cancellation_of_first_or_latest_wave_pauses(self):
        for latest in (False, True):
            api, saved = self.ready()
            run_id = 101
            if latest:
                run_id = 103
                api.runs[103] = api.run(103, dict(campaign_id="101"))
                saved["waves"].append(dict(run_id=103))
            api.finish(run_id, "cancelled")
            self.assertEqual(self.advance(api)["status"], "paused")
            self.assertEqual(len(api.dispatches()), 1)

    def test_incomplete_publication_and_conflicting_platform_link_block(self):
        changes = [dict(blocked=["task"]), dict(publication_pending=True),
                   dict(completed=None), dict(status="unknown"),
                   dict(next_platform_campaign=dict(campaign_id="90")),
                   dict(next_platform_campaign=dict(campaign_id="90"), next_platform_dispatched="saved")]
        for change in changes:
            api, saved = self.ready()
            saved.update(change)
            self.assertEqual(self.advance(api)["status"], "blocked")
            self.assertEqual(len(api.dispatches()), 1)
        api, saved = self.ready()
        api.finish(101, "failure")
        self.assertEqual(self.advance(api)["status"], "blocked")

    def test_state_identity_history_and_controller_are_checked(self):
        changes = [dict(campaign_id="wrong"), dict(controller_ref="c" * 40),
                   dict(waves=[]), dict(waves=[dict(run_id=999)]),
                   dict(waves=[dict(run_id="101")])]
        for change in changes:
            api, saved = self.ready()
            saved.update(change)
            self.assertEqual(self.advance(api)["status"], "blocked")
            self.assertEqual(len(api.dispatches()), 1)

    def test_workflow_repository_source_and_title_are_checked(self):
        for change in (dict(path=".github/workflows/benchmark.yml"),
                       dict(repository=dict(full_name="foreign/repo")),
                       dict(head_repository=dict(full_name="foreign/repo")),
                       dict(event="push")):
            api, saved = self.ready()
            api.runs[101].update(change)
            with self.assertRaises(ValueError):
                self.advance(api)
            self.assertEqual(len(api.dispatches()), 1)
        for change in (dict(head_sha="c" * 40), dict(display_title="wrong platform")):
            api, saved = self.ready()
            api.runs[101].update(change)
            self.assertEqual(self.advance(api)["status"], "blocked")

    def test_wrong_dispatch_tag_blocks_before_post(self):
        api = FakeParallelAPI()
        api.ref_sha = "c" * 40
        self.assertEqual(self.advance(api)["status"], "blocked")
        self.assertFalse(api.dispatches())

    def test_accepted_dispatch_timeout_recovers_without_duplicate(self):
        api = FakeParallelAPI()
        api.dispatch_error = TimeoutError("Response lost")
        api.accept_before_error = True
        with self.assertRaises(TimeoutError):
            self.advance(api)
        self.assertIn("dispatch_intent", api.state)
        self.assertEqual(self.advance(api)["status"], "dispatched")
        self.assertNotIn("dispatch_intent", api.state)
        self.assertEqual(self.advance(api)["status"], "waiting")
        self.assertEqual(len(api.dispatches()), 1)
        writes = [index for index, call in enumerate(api.calls) if call[0] == "PUT"]
        post = next(index for index, call in enumerate(api.calls) if call[0] == "POST")
        self.assertLess(writes[0], post)

    def test_missing_dispatch_receipt_recovers_without_duplicate(self):
        api = FakeParallelAPI()
        api.empty_receipt = True
        self.assertEqual(self.advance(api)["status"], "blocked")
        self.assertEqual(self.advance(api)["status"], "dispatched")
        self.assertEqual(len(api.dispatches()), 1)

    def test_uncertain_or_ambiguous_dispatch_never_reposts(self):
        for accepted in (False, True):
            api = FakeParallelAPI()
            api.dispatch_error = TimeoutError("Uncertain dispatch")
            api.accept_before_error = accepted
            with self.assertRaises(TimeoutError):
                self.advance(api)
            if accepted:
                api.runs[102] = dict(api.runs[101], id=102)
            self.assertEqual(self.advance(api)["status"], "blocked")
            self.assertEqual(self.advance(api)["status"], "blocked")
            self.assertEqual(len(api.dispatches()), 1)

    def test_resume_controls_and_tampered_intent_are_rejected(self):
        for change in (dict(order="8"), dict(software="acousticts"), dict(mode="repeat")):
            saved = parallel_state()
            saved["requests"][1]["inputs"].update(change)
            with self.assertRaises(ValueError):
                queue.validate_state(saved, "owner/repo")
        for change in (dict(source_ref="c" * 40), dict(inputs=dict(campaign_id="99"))):
            saved = parallel_state()
            saved["dispatch_intent"] = dict(saved["requests"][0], created_at=queue.utc_now())
            saved["dispatch_intent"].update(change)
            with self.assertRaises(ValueError):
                queue.validate_state(saved, "owner/repo")

    def test_ordered_receipts_and_metadata_are_validated(self):
        api, saved = self.ready()
        for change in (dict(source_ref="c" * 40), dict(campaign_id="102"), dict(run_id="101")):
            altered = copy.deepcopy(api.state)
            altered["dispatched"]["echo-existing"].update(change)
            with self.assertRaises(ValueError):
                queue.validate_state(altered, "owner/repo")
        for change in (dict(source_ref=None), dict(dispatch_ref=None), dict(key="bad key")):
            altered = parallel_state()
            altered["requests"][0].update(change)
            with self.assertRaises(ValueError):
                queue.validate_state(altered, "owner/repo")
        altered = parallel_state()
        altered["dispatched"] = {"unknown": {}}
        with self.assertRaises(ValueError):
            queue.validate_state(altered, "owner/repo")

    def test_queue_save_conflict_prevents_dispatch(self):
        api = FakeParallelAPI()
        with patch.object(queue, "save_queue", side_effect=RuntimeError("Concurrent update")):
            with self.assertRaises(RuntimeError):
                self.advance(api)
        self.assertFalse(api.dispatches())



class ParallelControllerUpgradeTests(unittest.TestCase):
    advance = ParallelQueueTests.advance
    ready = ParallelQueueTests.ready

    def approved(self, active=False):
        api, saved = self.ready()
        request = api.state["requests"][0]
        request["approved_controllers"] = ["a" * 40, "c" * 40]
        files = ["src/parallel_campaign.py", "tests/test_parallel_campaign.py"]
        saved.update(controller_ref="c" * 40, controller_history=[
            dict(previous="a" * 40, current="c" * 40, files=files, changed_at=queue.utc_now())])
        api.comparisons["/compare/" + "a" * 40 + "..." + "c" * 40] = dict(
            status="ahead", files=[dict(filename=path, status="modified") for path in files])
        api.runs[103] = dict(api.run(103, dict(campaign_id="101")), head_sha="c" * 40)
        if active:
            api.runs[103].update(status="in_progress", conclusion=None)
        saved["waves"].append(dict(run_id=103))
        return api, saved

    def test_pending_handoff_keeps_original_controller_and_receipt_valid(self):
        api, saved = self.ready()
        original = copy.deepcopy(api.state["dispatched"]["echo-existing"])
        api.state["requests"][0]["approved_controllers"] = ["a" * 40, "c" * 40]
        saved.update(status="active", controller_tag="new-scheduler-tag",
                     controller_upgrade_pending=dict(target="c" * 40))
        self.assertEqual(self.advance(api)["status"], "waiting")
        self.assertEqual(api.state["dispatched"]["echo-existing"], original)
        self.assertFalse(any(path.startswith("/compare/") for _, path, _ in api.calls))
        self.assertEqual(len(api.dispatches()), 1)

    def test_verified_upgrade_waits_then_advances_without_rewriting_original_receipt(self):
        api, saved = self.approved(active=True)
        original = copy.deepcopy(api.state["dispatched"]["echo-existing"])
        self.assertEqual(self.advance(api)["status"], "waiting")
        self.assertEqual(len(api.dispatches()), 1)
        api.finish(103)
        api.ref_sha = "b" * 40
        self.assertEqual(self.advance(api)["campaign_id"], "77")
        self.assertEqual(api.state["dispatched"]["echo-existing"], original)
        self.assertEqual(api.runs[101]["head_sha"], "a" * 40)
        self.assertEqual(api.state["dispatched"]["acousticts-macos"]["source_ref"], "b" * 40)

    def test_partial_approved_chain_and_preexisting_history_remain_valid(self):
        api, saved = self.approved(active=True)
        api.state["requests"][0]["approved_controllers"].append("d" * 40)
        saved["controller_history"].insert(0, dict(previous="f" * 40, current="a" * 40,
            files=["src/parallel_campaign.py"]))
        self.assertEqual(self.advance(api)["status"], "waiting")
        saved["controller_history"].append(dict(previous="c" * 40, current="d" * 40,
            files=["src/parallel_campaign.py"], changed_at=queue.utc_now()))
        saved["controller_ref"] = "d" * 40
        api.comparisons["/compare/" + "c" * 40 + "..." + "d" * 40] = dict(
            status="ahead", files=[dict(filename="src/parallel_campaign.py", status="modified")])
        api.runs[104] = dict(api.run(104, dict(campaign_id="101")), head_sha="d" * 40,
                             status="in_progress", conclusion=None)
        saved["waves"].append(dict(run_id=104))
        self.assertEqual(self.advance(api)["status"], "waiting")

    def test_invalid_approval_chains_are_rejected_before_dispatch(self):
        bad = [None, "c" * 40, [], ["c" * 40], ["a" * 40, "a" * 40],
               ["a" * 40, "bad"], ["a" * 40, 1], ["a" * 40, []],
               ["a" * 40] + [format(i, "040x") for i in range(20)]]
        for controllers in bad:
            with self.subTest(controllers=controllers):
                api = FakeParallelAPI()
                api.state["requests"][0]["approved_controllers"] = controllers
                with self.assertRaisesRegex(ValueError, "approved parallel controller"):
                    self.advance(api)
                self.assertFalse(api.dispatches())

    def test_controller_history_requires_exact_contiguous_approved_transitions(self):
        mutations = [
            lambda saved: saved.pop("controller_history"),
            lambda saved: saved.update(controller_history={}),
            lambda saved: saved.update(controller_history=[None]),
            lambda saved: saved["controller_history"][0].update(previous="b" * 40),
            lambda saved: saved["controller_history"][0].update(current="d" * 40),
            lambda saved: saved["controller_history"][0].update(files=[]),
            lambda saved: saved["controller_history"][0].update(files=[None]),
            lambda saved: saved["controller_history"][0].update(files=["src/parallel_campaign.py"]),
            lambda saved: saved["controller_history"][0]["files"].append("src/parallel_campaign.py"),
            lambda saved: saved.update(controller_ref="d" * 40),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                api, saved = self.approved()
                mutate(saved)
                self.assertEqual(self.advance(api)["status"], "blocked")
                self.assertEqual(len(api.dispatches()), 1)

    def test_actual_numerical_changes_block_even_with_approved_sha_and_claimed_history(self):
        protected = ["sources.lock.json", "benchmark.json", "src/benchmark.py",
                     "src/worker_python.py", "src/worker_acousticts.R",
                     ".github/workflows/parallel-spectra.yml"]
        for path in protected:
            with self.subTest(path=path):
                api, saved = self.approved()
                comparison = next(iter(api.comparisons.values()))
                comparison["files"].append(dict(filename=path, status="modified"))
                saved["controller_history"][0]["files"].append(path)
                self.assertEqual(self.advance(api)["status"], "blocked")
                self.assertEqual(len(api.dispatches()), 1)

    def test_behind_empty_renamed_and_truncated_comparisons_are_rejected(self):
        comparisons = [
            dict(status="behind", files=[dict(filename="src/parallel_campaign.py", status="modified")]),
            dict(status="identical", files=[]),
            dict(status="ahead", files=[]),
            dict(status="ahead", files=None),
            dict(status="ahead", files=[None]),
            dict(status="ahead", files=[dict(filename="src/parallel_campaign.py", status="renamed")]),
            dict(status="ahead", files=[dict(filename="src/parallel_campaign.py", status="modified")] * 300),
        ]
        for comparison in comparisons:
            with self.subTest(comparison=comparison):
                api, saved = self.approved()
                api.comparisons[next(iter(api.comparisons))] = comparison
                self.assertEqual(self.advance(api)["status"], "blocked")
                self.assertEqual(len(api.dispatches()), 1)

    def test_original_and_latest_workflow_sources_remain_distinct_checks(self):
        for identity, source_ref in ((101, "c" * 40), (103, "a" * 40)):
            with self.subTest(identity=identity):
                api, saved = self.approved()
                api.runs[identity]["head_sha"] = source_ref
                self.assertEqual(self.advance(api)["status"], "blocked")
                self.assertEqual(len(api.dispatches()), 1)

    def test_unapproved_change_and_history_rollback_are_blocked(self):
        api, saved = self.approved()
        api.state["requests"][0].pop("approved_controllers")
        self.assertEqual(self.advance(api)["status"], "blocked")
        api, saved = self.approved()
        saved["controller_ref"] = "a" * 40
        api.runs[103]["head_sha"] = "a" * 40
        self.assertEqual(self.advance(api)["status"], "blocked")

    def test_extra_unapproved_history_cannot_be_hidden_before_matching_suffix(self):
        api, saved = self.approved()
        saved["controller_history"][0:0] = [
            dict(previous="a" * 40, current="e" * 40, files=["benchmark.json"]),
            dict(previous="e" * 40, current="a" * 40, files=["benchmark.json"])]
        self.assertEqual(self.advance(api)["status"], "blocked")
        self.assertEqual(len(api.dispatches()), 1)

    def test_comparison_read_failure_never_changes_queue_or_dispatches(self):
        api, saved = self.approved()
        original = copy.deepcopy(api.state)
        request = api.request
        def fail_compare(method, path, body=None):
            if path.startswith("/compare/"):
                raise TimeoutError("Comparison unavailable")
            return request(method, path, body)
        with patch.object(api, "request", side_effect=fail_compare):
            with self.assertRaises(TimeoutError):
                self.advance(api)
        self.assertEqual(api.state, original)
        self.assertEqual(len(api.dispatches()), 1)

    def test_dispatch_intent_cannot_change_controller_approval(self):
        for add_to_request in (False, True):
            api = FakeParallelAPI()
            if not add_to_request:
                api.state["requests"][0]["approved_controllers"] = ["a" * 40, "c" * 40]
            api.dispatch_error = TimeoutError("Uncertain dispatch")
            with self.assertRaises(TimeoutError):
                self.advance(api)
            if add_to_request:
                api.state["requests"][0]["approved_controllers"] = ["a" * 40, "c" * 40]
            else:
                api.state["requests"][0].pop("approved_controllers")
            with self.assertRaisesRegex(ValueError, "dispatch intent"):
                self.advance(api)
            self.assertEqual(len(api.dispatches()), 1)


class ParallelInterruptedRunTests(unittest.TestCase):
    advance = ParallelQueueTests.advance
    ready = ParallelQueueTests.ready
    approved = ParallelControllerUpgradeTests.approved

    def resumed(self, active=True):
        api, saved = self.approved(active=active)
        api.state["requests"][0]["interrupted_runs"] = [101]
        api.finish(101, "cancelled")
        saved["status"] = "active" if active else "complete"
        return api, saved

    def test_authorized_first_cancellation_waits_for_verified_resume_then_advances(self):
        api, saved = self.resumed()
        receipt = copy.deepcopy(api.state["dispatched"]["echo-existing"])
        self.assertEqual(self.advance(api)["status"], "waiting")
        self.assertEqual(len(api.dispatches()), 1)
        api.finish(103)
        saved["status"] = "complete"
        api.ref_sha = "b" * 40
        self.assertEqual(self.advance(api)["campaign_id"], "77")
        self.assertEqual(api.state["dispatched"]["echo-existing"], receipt)

    def test_cancelled_latest_wave_still_pauses_with_or_without_listing(self):
        for listed in (False, True):
            with self.subTest(listed=listed):
                api, saved = self.resumed()
                api.finish(103, "cancelled")
                if listed:
                    api.state["requests"][0]["interrupted_runs"].append(103)
                self.assertEqual(self.advance(api)["status"], "paused")
                self.assertEqual(len(api.dispatches()), 1)
        api, saved = self.ready()
        api.state["requests"][0]["interrupted_runs"] = [101]
        api.finish(101, "cancelled")
        self.assertEqual(self.advance(api)["status"], "paused")
        self.assertEqual(len(api.dispatches()), 1)

    def test_cancelled_middle_wave_requires_explicit_listing(self):
        for listed in (False, True):
            with self.subTest(listed=listed):
                api, saved = self.resumed()
                api.finish(103, "cancelled")
                api.runs[104] = dict(api.run(104, dict(campaign_id="101")), head_sha="c" * 40,
                                     status="in_progress", conclusion=None)
                saved["waves"].append(dict(run_id=104))
                if listed:
                    api.state["requests"][0]["interrupted_runs"].append(103)
                self.assertEqual(self.advance(api)["status"], "waiting" if listed else "paused")
                self.assertEqual(len(api.dispatches()), 1)

    def test_interruption_does_not_allow_wrong_source_or_wrong_campaign_resume(self):
        changes = [dict(head_sha="d" * 40), dict(display_title="Continue spectra 999")]
        for change in changes:
            with self.subTest(change=change):
                api, saved = self.resumed()
                api.runs[103].update(change)
                self.assertEqual(self.advance(api)["status"], "blocked")
                self.assertEqual(len(api.dispatches()), 1)
        api, saved = self.resumed()
        api.runs[101]["head_sha"] = "c" * 40
        self.assertEqual(self.advance(api)["status"], "blocked")

    def test_interruption_does_not_allow_unapproved_or_foreign_middle_wave(self):
        for source, title in (("d" * 40, "Continue spectra 101"),
                              ("c" * 40, "Continue spectra 999")):
            with self.subTest(source=source, title=title):
                api, saved = self.resumed()
                api.finish(103, "cancelled")
                api.runs[103].update(head_sha=source, display_title=title)
                api.runs[104] = dict(api.run(104, dict(campaign_id="101")), head_sha="c" * 40)
                saved["waves"].append(dict(run_id=104))
                api.state["requests"][0]["interrupted_runs"].append(103)
                self.assertEqual(self.advance(api)["status"], "blocked")
                self.assertEqual(len(api.dispatches()), 1)

    def test_unknown_uncancelled_duplicate_or_nonnewer_history_cannot_bypass_pause(self):
        for mode in ("unknown", "uncancelled", "duplicate", "nonnewer"):
            with self.subTest(mode=mode):
                api, saved = self.resumed()
                if mode == "unknown":
                    api.state["requests"][0]["interrupted_runs"].append(999)
                elif mode == "uncancelled":
                    api.finish(101)
                elif mode == "duplicate":
                    saved["waves"].insert(1, dict(run_id=101))
                else:
                    api.runs[99] = dict(api.run(99, dict(campaign_id="101")), head_sha="c" * 40,
                                        status="in_progress", conclusion=None)
                    saved["waves"][-1] = dict(run_id=99)
                self.assertEqual(self.advance(api)["status"], "blocked")
                self.assertEqual(len(api.dispatches()), 1)

    def test_invalid_interrupted_metadata_is_rejected(self):
        values = [None, "101", ["101"], [True], [0], [-1], [101, 101], list(range(1, 102))]
        for value in values:
            with self.subTest(value=value):
                api, saved = self.ready()
                api.state["requests"][0]["interrupted_runs"] = value
                with self.assertRaisesRegex(ValueError, "interrupted parallel run"):
                    self.advance(api)
                self.assertEqual(len(api.dispatches()), 1)
        api = FakeParallelAPI()
        api.state["requests"][0]["interrupted_runs"] = [101]
        with self.assertRaisesRegex(ValueError, "existing stage receipt"):
            self.advance(api)
        self.assertFalse(api.dispatches())

    def test_interruption_approval_does_not_stop_an_active_original_run(self):
        api, saved = self.ready()
        api.state["requests"][0]["interrupted_runs"] = [101]
        api.runs[101].update(status="in_progress", conclusion=None)
        self.assertEqual(self.advance(api)["status"], "waiting")
        self.assertEqual(len(api.dispatches()), 1)

    def test_interruption_cannot_be_added_to_an_uncertain_dispatch_intent(self):
        api = FakeParallelAPI()
        api.dispatch_error = TimeoutError("Uncertain dispatch")
        with self.assertRaises(TimeoutError):
            self.advance(api)
        api.state["dispatch_intent"]["interrupted_runs"] = [101]
        with self.assertRaisesRegex(ValueError, "dispatch intent"):
            self.advance(api)
        self.assertEqual(len(api.dispatches()), 1)


if __name__ == "__main__":
    unittest.main()

