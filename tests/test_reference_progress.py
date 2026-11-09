"""Terminal reference progress shows literal cases, counters and stalled workers."""
import sys
import os
import json
import tempfile
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from reference_progress import ProgressReporter, shared_snapshot_reader, read_snapshot


def fixture():
    job = dict(reference_id="case", boundary="gas", semimajor_m=.01,
               aspect_ratio=1.001, frequency_hz=37000, incidence_deg=37)
    manifest = dict(requests=[job], skipped_existing_physical_ids=["old"], planned=2)
    state = dict(state="running", controller_pid=123, attempted={}, published=[],
                 history=[dict(action="start", at="1970-01-01T00:01:40+00:00")])
    active = dict(reference_id="case", started_epoch=100, phase="solving")
    return manifest, state, active


class ReferenceProgressTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "nt", "Windows sharing regression")
    def test_shared_reader_closes_handles_before_subsequent_replacement(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "status.json"
            replacement = Path(directory) / "status.json.tmp"
            path.write_text('{"generation":1}', encoding="utf-8")
            replacement.write_text('{"generation":2}', encoding="utf-8")
            with shared_snapshot_reader(path) as reader:
                self.assertEqual(json.load(reader), {"generation": 1})
            # The context releases the descriptor and Windows handle.
            os.replace(replacement, path)
            self.assertEqual(read_snapshot(path), {"generation": 2})
            path.unlink()
            self.assertEqual(read_snapshot(path, {"missing": True}), {"missing": True})

    def test_running_resume_does_not_repeat_historical_controller_error(self):
        manifest, state, active = fixture()
        state["last_error"] = "old PermissionError"
        report = ProgressReporter(manifest)
        output = "\n".join(report.update(state, active, now=105))
        self.assertNotIn("CONTROLLER ERROR", output)
        state["state"] = "stopped_after_error"
        output = "\n".join(report.update(state, active, now=106))
        self.assertIn("CONTROLLER ERROR old PermissionError", output)

    def test_literal_geometry_counts_and_unchanged_phase_heartbeat(self):
        manifest, state, active = fixture()
        report = ProgressReporter(manifest)
        output = "\n".join(report.update(state, active, now=105))
        for expected in ("total=2 existing=1 to_calculate=1", "a/b=1.001", "f=37.00 kHz",
                         "angle=37 deg", "attempted=0/1", "remaining=1", "case_elapsed=00:00:05"):
            self.assertIn(expected, output)
        self.assertEqual(report.update(state, active, now=119), [])
        self.assertIn("case_elapsed=00:00:20", "\n".join(report.update(state, active, now=120)))

    def test_result_upload_and_stale_marker(self):
        manifest, state, active = fixture()
        report = ProgressReporter(manifest)
        report.update(state, active, now=100)
        state["attempted"] = {"case": dict(qualified=True, status="qualified")}
        output = "\n".join(report.update(state, active, now=101))
        self.assertIn("QUALIFIED status=qualified", output)
        self.assertIn("qualified=1 failed=0 remaining=0", output)
        self.assertIn("upload_pending=1", output)
        self.assertNotIn("phase=solving", output)
        state["published"] = ["case"]
        state["state"] = "completed"
        output = "\n".join(report.update(state, active, now=102, alive=False))
        self.assertIn("UPLOADED +1 uploaded=1 pending=0", output)
        self.assertTrue(report.finished)

    def test_failure_publication_error_and_dead_controller_are_explicit(self):
        manifest, state, active = fixture()
        report = ProgressReporter(manifest)
        report.update(state, active, now=100)
        state["attempted"] = {"case": dict(qualified=False, status="timeout")}
        state["publication_errors"] = [dict(at="fixture", error="HTTPError")]
        output = "\n".join(report.update(state, active, now=101, alive=False))
        self.assertIn("FAILED status=timeout", output)
        self.assertIn("UPLOAD FAILED HTTPError", output)
        self.assertIn("CONTROLLER_STOPPED", output)
        self.assertIn("qualified=0 failed=1", output)
        self.assertTrue(report.finished)

    def test_error_state_keeps_monitor_open_until_final_upload_finishes(self):
        manifest, state, active = fixture()
        state.update(state="stopped_after_error", last_error="PermissionError",
                     attempted={"case": dict(qualified=True, status="ok")})
        report = ProgressReporter(manifest)
        report.update(state, active, now=100, alive=True)
        self.assertFalse(report.finished)
        state["published"] = ["case"]
        output = "\n".join(report.update(state, active, now=101, alive=False))
        self.assertIn("UPLOADED +1 uploaded=1 pending=0", output)
        self.assertTrue(report.finished)

    def test_old_marker_and_planned_state_do_not_claim_active_case(self):
        manifest, state, active = fixture()
        report = ProgressReporter(manifest)
        active["started_epoch"] = 50
        output = "\n".join(report.update(state, active, now=105))
        self.assertNotIn("phase=solving", output)
        state["state"] = "planned"
        active["started_epoch"] = 100
        output = "\n".join(report.update(state, active, now=106))
        self.assertIn("PLANNED", output)
        self.assertNotIn("phase=solving", output)


if __name__ == "__main__":
    unittest.main()
