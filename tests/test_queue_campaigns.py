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


if __name__ == "__main__":
    unittest.main()
