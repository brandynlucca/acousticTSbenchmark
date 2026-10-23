"""Public snapshot content, artifact choice, and concurrent publication guards."""

import base64
import copy
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import publish_progress as progress


class ProgressTests(unittest.TestCase):
    def payload(self, run=12, attempt=1, state="running"):
        return progress.compact_payload("owner/repo", "ubuntu-latest", run, attempt,
            [{"software": "acousticts", "status": "ok", "ts_db": -30,
              "elapsed_seconds": 1.2, "diagnostics": {"private_path": "/private"}},
             {"software": "acousticts", "status": "pending"}],
            {"planned": 2, "recorded": 1, "state": "budget_paused", "private_path": "/private"}, state)

    def test_public_payload_omits_pending_and_private_diagnostics(self):
        payload = self.payload()
        self.assertEqual(payload["status"], {"planned": 2, "recorded": 1, "state": "budget_paused",
                                             "outcomes": {"ok": 1, "pending": 1}})
        self.assertEqual(len(payload["rows"]), 1)
        self.assertEqual(set(payload["rows"][0]), set(progress.ROW_FIELDS))
        self.assertNotIn("private", json.dumps(payload))
        self.assertEqual(payload["artifacts_url"], "https://github.com/owner/repo/actions/runs/12#artifacts")
        invalid = progress.compact_payload("owner/repo", "ubuntu-latest", 12, 1,
            [{"status": "numerical_error", "elapsed_seconds": float("nan")}])
        self.assertIsNone(invalid["rows"][0]["elapsed_seconds"])
        json.dumps(invalid, allow_nan=False)

    def test_axes_include_pending_frequencies_and_recorded_failures(self):
        payload = progress.compact_payload("owner/repo", "ubuntu-latest", 12, 1, [
            dict(status="ok", aspect_ratio=10, incidence_deg=0, frequency_hz=1000, h0=.1, k0a=.2),
            dict(status="setup_timeout", aspect_ratio=10, incidence_deg=37, frequency_hz=3000, h0=.3, k0a=.6),
            dict(status="pending", aspect_ratio=10, incidence_deg=90, frequency_hz=200000, h0=20, k0a=40)])
        self.assertEqual(payload["axes"]["10"], dict(frequency_hz=[1000, 200000], h0=[.1, 20],
            k0a=[.2, 40], frequencies_hz=[1000, 3000, 200000]))
        self.assertEqual(payload["aspect_ratios"], [10])
        self.assertEqual(payload["incidences_deg"], [0, 37, 90])
        self.assertEqual(payload["status_counts"], {"ok": 1, "setup_timeout": 1})

    def test_coverage_retains_pending_only_groups_and_interior_frequency_gaps(self):
        rows = [dict(software="acousticts", boundary="gas", aspect_ratio=10,
                     frequency_hz=frequency, incidence_deg=90, status=status, elapsed_seconds=elapsed)
                for frequency, status, elapsed in ((1000, "ok", 1.0), (3000, "pending", None),
                                                  (5000, "ok", 2.0), (7000, "timeout", None))]
        rows.append(dict(software="echosms", boundary="rigid", aspect_ratio=10,
                         frequency_hz=1000, status="pending"))
        payload = progress.compact_payload("owner/repo", "ubuntu-latest", 12, 1, rows)
        self.assertEqual(payload["axes"]["10"]["frequencies_hz"], [1000, 3000, 5000, 7000])
        groups = {group["software"]: group for group in payload["coverage"]}
        self.assertEqual(groups["acousticts"], dict(software="acousticts", boundary="gas", aspect_ratio=10,
            planned=4, recorded=3, success=2, other_recorded=1, pending=1, elapsed_seconds_sum=3.0))
        self.assertEqual(groups["echosms"]["pending"], 1)
        self.assertEqual(groups["echosms"]["recorded"], 0)
        self.assertEqual(len(payload["rows"]), 3)

    def test_older_runs_attempts_and_regressive_snapshots_are_not_published(self):
        current = self.payload(12, 2)
        self.assertFalse(progress.should_publish(current, self.payload(11, 5)))
        self.assertFalse(progress.should_publish(current, self.payload(12, 1)))
        self.assertTrue(progress.should_publish(current, self.payload(13, 1)))
        empty = copy.deepcopy(current)
        empty["status"]["recorded"] = 0
        self.assertFalse(progress.should_publish(current, empty))
        finished = self.payload(12, 2, "completed")
        self.assertFalse(progress.should_publish(finished, current))

    def test_latest_artifact_filters_platform_expiration_and_names(self):
        artifacts = [{"id": 1, "name": "checkpoint-ubuntu-latest-1-1"},
                     {"id": 2, "name": "results-ubuntu-latest"},
                     {"id": 3, "name": "checkpoint-ubuntu-latest-1-2", "expired": True},
                     {"id": 4, "name": "checkpoint-windows-latest-1-2"},
                     {"id": 5, "name": "checkpoint-ubuntu-latest-1-2-extra"},
                     {"id": 6, "name": "environment-ubuntu-latest"}]
        self.assertEqual(progress.select_artifact(artifacts, "ubuntu-latest")["id"], 2)
        self.assertIsNone(progress.select_artifact(artifacts, "macos-latest"))

    def test_artifact_import_uses_source_run_and_checkpoint_attempt(self):
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as output:
            output.writestr("results.json", json.dumps([{"status": "ok", "ts_db": -12}]))
            output.writestr("status.json", json.dumps({"planned": 4, "recorded": 1}))
            output.writestr("records/private.json", "private content")
        api = Mock(repository="owner/repo")
        api.request.side_effect = [{"status": "completed", "run_attempt": 2},
            {"artifacts": [{"id": 7, "name": "checkpoint-ubuntu-latest-1-2"}]}]
        api.archive.return_value = archive.getvalue()
        payload = progress.artifact_payload(api, "ubuntu-latest", 99)
        self.assertEqual((payload["run_id"], payload["run_attempt"]), (99, 1))
        self.assertEqual(payload["state"], "completed")
        self.assertEqual(payload["artifact_id"], 7)
        self.assertNotIn("private", json.dumps(payload))

    def test_run_without_checkpoint_publishes_setup_without_measurements(self):
        api = Mock(repository="owner/repo")
        api.request.side_effect = [{"status": "in_progress", "run_attempt": 1, "conclusion": None},
                                   {"artifacts": []}]
        payload = progress.artifact_payload(api, "ubuntu-latest", 99)
        self.assertEqual(payload["state"], "setup")
        self.assertEqual(payload["run_state"], "in_progress")
        self.assertIsNone(payload["run_conclusion"])
        self.assertEqual(payload["rows"], [])
        self.assertEqual(payload["status"]["recorded"], 0)
        api.archive.assert_not_called()

    def test_artifact_redirect_does_not_forward_github_token(self):
        api = progress.GitHub("owner/repo", "test-token")
        signed = "https://example.blob.core.windows.net/artifact?signature=example"
        api.opener = Mock()
        api.opener.open.side_effect = [
            HTTPError("https://api.github.com/artifact", 302, "Found", {"Location": signed}, None),
            io.BytesIO(b"archive")]
        self.assertEqual(api.archive(7), b"archive")
        requests = [call.args[0] for call in api.opener.open.call_args_list]
        self.assertEqual(requests[0].get_header("Authorization"), "Bearer test-token")
        self.assertIsNone(requests[1].get_header("Authorization"))
        self.assertEqual(requests[1].full_url, signed)

    def test_conflicting_update_rereads_and_preserves_newer_run(self):
        proposed, newer = self.payload(12), self.payload(13)

        def content(payload, sha):
            return dict(sha=sha, encoding="base64",
                        content=base64.b64encode(json.dumps(payload).encode()).decode())

        api = Mock()
        api.request.side_effect = [{"ref": "exists"}, content(self.payload(11), "old-sha"),
            progress.APIError(409, "Conflict"), {"ref": "exists"}, content(newer, "new-sha")]
        self.assertFalse(progress.publish(api, proposed))
        puts = [call for call in api.request.call_args_list if call.args[0] == "PUT"]
        self.assertEqual(len(puts), 1)
        self.assertEqual(puts[0].args[2]["sha"], "old-sha")

    def test_large_snapshot_uses_blob_when_contents_omits_inline_data(self):
        payload = dict(self.payload(), padding="x" * (1024 * 1024))
        serialized = json.dumps(payload).encode()
        api = Mock()
        api.request.side_effect = [dict(sha="large-sha", encoding="none", content="", size=len(serialized)),
            dict(sha="large-sha", encoding="base64", size=len(serialized),
                 content=base64.b64encode(serialized).decode())]
        restored, sha = progress.existing_snapshot(api, "ubuntu-latest")
        self.assertEqual(restored, payload)
        self.assertEqual(sha, "large-sha")
        self.assertEqual(api.request.call_args_list[1].args, ("GET", "/git/blobs/large-sha"))

    def test_snapshot_size_is_checked_before_fetch_and_after_encoding(self):
        api = Mock()
        api.request.return_value = dict(sha="large-sha", encoding="none", content="",
                                       size=progress.MAX_SNAPSHOT_BYTES + 1)
        with self.assertRaisesRegex(ValueError, "size"):
            progress.existing_snapshot(api, "ubuntu-latest")
        self.assertEqual(api.request.call_count, 1)
        api.request.return_value = dict(sha="large-sha", encoding="base64", size=0,
                                       content=base64.b64encode(b"x" * 129).decode())
        with patch.object(progress, "MAX_SNAPSHOT_BYTES", 128):
            with self.assertRaisesRegex(ValueError, "size"):
                progress.existing_snapshot(api, "ubuntu-latest")

    def test_bootstrap_commit_contains_only_platform_data_and_no_parent(self):
        api = Mock()
        api.request.side_effect = [progress.APIError(404, "Missing"), {"sha": "blob"},
                                   {"sha": "tree"}, {"sha": "commit"}, {"ref": "created"}]
        self.assertTrue(progress.publish(api, self.payload()))
        tree_call = api.request.call_args_list[2]
        self.assertEqual(tree_call.args[2]["tree"], [dict(path="ubuntu-latest.json", mode="100644",
                                                         type="blob", sha="blob")])
        self.assertEqual(api.request.call_args_list[3].args[2]["parents"], [])


if __name__ == "__main__":
    unittest.main()
