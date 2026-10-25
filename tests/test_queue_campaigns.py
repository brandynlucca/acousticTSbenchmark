"""Durable benchmark sequencing without resubmitting uncertain dispatches."""

import base64
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import queue_campaigns as queue


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
            self.runs[identity] = dict(self.run(identity, body["inputs"]), status="pending", conclusion=None)
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

    def dispatches(self):
        return [body for method, path, body in self.calls if method == "POST" and path.endswith("/dispatches")]


class QueueTests(unittest.TestCase):
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

    def test_failure_with_retained_baseline_can_continue(self):
        api = FakeAPI()
        api.finish(100, "failure", "retained-windows-latest")
        self.assertEqual(queue.advance(api)["status"], "dispatched")

    def test_no_baseline_or_expired_results_pause(self):
        for entries in ([], [dict(name="environment-ubuntu-latest")],
                        [dict(name="results-ubuntu-latest", expired=True)]):
            with self.subTest(entries=entries):
                api = FakeAPI()
                api.artifacts[100] = entries
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


if __name__ == "__main__":
    unittest.main()
