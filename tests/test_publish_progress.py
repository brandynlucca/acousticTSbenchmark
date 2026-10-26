"""Public snapshot content, artifact choice, and concurrent publication guards."""

import base64
import copy
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import publish_progress as progress


class ProgressTests(unittest.TestCase):
    def setUp(self):
        scratch = progress.ROOT / ".scratch/codex/dense-benchmark/tests"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def campaign(self):
        return dict(geometry=dict(semimajor_m=.07, aspect_ratios=[1.001, 1.01, 5, 10, 50, 100]),
            incidences_deg=[0, 37, 90], boundaries=["rigid", "pressure_release", "liquid", "gas"],
            software=["acousticts", "echosms"], repetitions=1, warmup=True,
            controls=dict(acousticts={}, echosms={}),
            frequency=dict(start_hz=1000, stop_hz=200000, step_hz=2000, include_stop=True),
            medium=dict(density_kg_m3=1026.8, sound_speed_m_s=1477.3),
            materials=dict(liquid=dict(density_kg_m3=1028.9, sound_speed_m_s=1480.3),
                           gas=dict(density_kg_m3=1.24, sound_speed_m_s=345)),
            profiles=dict(pilot=dict(aspect_ratios=[10], incidences_deg=[90], boundaries=["gas"]), full={}))

    def config_file(self, configuration=None):
        path = self.directory / "campaign.json"
        path.write_text(json.dumps(self.campaign() if configuration is None else configuration), encoding="utf-8")
        return path

    def payload(self, run=12, attempt=1, state="running"):
        return progress.compact_payload("owner/repo", "ubuntu-latest", run, attempt,
            [{"software": "acousticts", "status": "ok", "ts_db": -30,
              "elapsed_seconds": 1.2, "diagnostics": {"private_path": "/private"}},
             {"software": "acousticts", "status": "pending"}],
            {"planned": 2, "recorded": 1, "state": "budget_paused", "private_path": "/private"}, state)

    def retained_fixture(self):
        configuration = self.campaign()
        configuration["frequency"] = dict(start_hz=1000, stop_hz=3000, step_hz=2000)
        active = [dict(case, status="pending") for case in progress.build_cases(
            configuration, "pilot", ["acousticts"], 1)]
        retained = [dict(case, status="ok", elapsed_seconds=.25, ts_db=-30,
                         source_sha="a" * 40, measurement_role="retained", retained_from_run_id=11)
                    for case in progress.build_cases(configuration, "pilot", ["echosms"], 1)]
        sidecar = dict(schema_version=1, rows=retained,
                       provenance=[dict(run_id=11, manifest_fingerprint="b" * 64)])
        return active, sidecar

    def test_retained_curves_do_not_advance_active_progress(self):
        active, sidecar = self.retained_fixture()
        path = self.directory / "retained-results.json"
        path.write_text(json.dumps(sidecar), encoding="utf-8")
        before = path.read_bytes()
        combined = progress.comparison_rows(self.directory, active)
        payload = progress.compact_payload("owner/repo", "ubuntu-latest", 12, 1, combined,
                                           dict(planned=2, recorded=0), "setup")
        self.assertEqual(payload["status"], dict(planned=2, recorded=0, outcomes=dict(pending=2)))
        self.assertEqual(payload["status_counts"], {})
        self.assertEqual(payload["active_software"], ["acousticts"])
        self.assertEqual(payload["retained_count"], 2)
        self.assertEqual(payload["retained_sources"], [dict(software="echosms", source_sha="a" * 40, run_id=11)])
        self.assertEqual({row["software"] for row in payload["rows"]}, {"echosms"})
        coverage = {row["software"]: row for row in payload["coverage"]}
        self.assertEqual(coverage["acousticts"]["pending"], 2)
        self.assertEqual(coverage["echosms"]["success"], 2)
        self.assertEqual(coverage["echosms"]["elapsed_seconds_sum"], .5)
        active[0].update(status="ok", elapsed_seconds=.1, ts_db=-29)
        next_payload = progress.compact_payload("owner/repo", "ubuntu-latest", 12, 1,
            progress.comparison_rows(self.directory, active), dict(planned=2), "running")
        self.assertEqual(next_payload["status"]["recorded"], 1)
        self.assertEqual(next_payload["status"]["outcomes"], dict(ok=1, pending=1))
        self.assertEqual(len(next_payload["rows"]), 3)
        self.assertTrue(progress.should_publish(payload, next_payload))
        self.assertEqual(path.read_bytes(), before)

    def test_retained_measurements_reject_pending_duplicates_and_active_software(self):
        active, original = self.retained_fixture()
        for patch in (dict(status="pending"), dict(measurement_role="active"),
                      dict(software="acousticts"), dict(source_sha="unknown"),
                      dict(retained_from_run_id=True), dict(retained_from_run_id=0),
                      dict(case_id=active[0]["case_id"])):
            with self.subTest(patch=patch):
                sidecar = copy.deepcopy(original)
                sidecar["rows"][0].update(patch)
                with self.assertRaisesRegex(ValueError, "retained measurement"):
                    progress.retained_comparison_rows(sidecar, active)
        duplicate = copy.deepcopy(original)
        duplicate["rows"].append(duplicate["rows"][0])
        with self.assertRaisesRegex(ValueError, "retained measurement"):
            progress.retained_comparison_rows(duplicate, active)
        for sidecar in ({}, dict(schema_version=1, rows=[], provenance="invalid")):
            with self.assertRaisesRegex(ValueError, "metadata"):
                progress.retained_comparison_rows(sidecar, active)

    def test_large_retained_history_uses_its_own_bound_without_rewriting_saved_data(self):
        active, sidecar = self.retained_fixture()
        sidecar["provenance"][0]["legacy_reference_plan"] = "x" * 20000
        path = self.directory / "retained-results.json"
        original = json.dumps(sidecar, indent=2).encode()
        path.write_bytes(original)
        with patch.object(progress, "MAX_JSON_BYTES", 10000), \
                patch.object(progress, "MAX_RETAINED_BYTES", len(original)):
            combined = progress.comparison_rows(self.directory, active)
            self.assertEqual(len(combined), 4)
        self.assertEqual(path.read_bytes(), original)
        with patch.object(progress, "MAX_RETAINED_BYTES", len(original) - 1):
            with self.assertRaisesRegex(ValueError, "supported size"):
                progress.comparison_rows(self.directory, active)

    def test_retained_writer_is_compact_and_rejects_oversize_before_replacing_a_baseline(self):
        _, sidecar = self.retained_fixture()
        path = self.directory / "retained-results.json"
        progress.write_retained(path, sidecar)
        encoded = path.read_bytes()
        self.assertEqual(json.loads(encoded), sidecar)
        self.assertLess(len(encoded), len(json.dumps(sidecar, indent=2)))
        with patch.object(progress, "MAX_RETAINED_BYTES", len(encoded) - 1):
            with self.assertRaisesRegex(ValueError, "supported size"):
                progress.write_retained(path, sidecar)
        self.assertEqual(path.read_bytes(), encoded)
        self.assertFalse(path.with_suffix(".json.tmp").exists())

    def test_artifact_progress_accepts_large_legacy_history_but_preserves_the_retained_bound(self):
        active, sidecar = self.retained_fixture()
        sidecar["provenance"][0]["legacy_reference_plan"] = "x" * 20000
        encoded = json.dumps(sidecar).encode()
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w") as archive:
            archive.writestr("results.json", json.dumps(active))
            archive.writestr("retained-results.json", encoded)
        api = Mock(repository="owner/repo")
        api.request.side_effect = lambda method, path: (dict(artifacts=[dict(id=7, name="results-ubuntu-latest")])
            if "/artifacts?" in path else dict(status="completed", run_attempt=1))
        api.archive.return_value = stream.getvalue()
        with patch.object(progress, "MAX_JSON_BYTES", 10000), \
                patch.object(progress, "MAX_RETAINED_BYTES", len(encoded)):
            payload = progress.artifact_payload(api, "ubuntu-latest", 12)
        self.assertEqual(payload["retained_count"], 2)
        self.assertEqual(payload["status"]["recorded"], 0)
        with patch.object(progress, "MAX_RETAINED_BYTES", len(encoded) - 1):
            with self.assertRaisesRegex(ValueError, "oversized"):
                progress.artifact_payload(api, "ubuntu-latest", 12)

    def test_setup_publishes_retained_curves_before_active_results_exist(self):
        _, sidecar = self.retained_fixture()
        (self.directory / "retained-results.json").write_text(json.dumps(sidecar), encoding="utf-8")
        configuration = self.campaign()
        configuration["frequency"] = dict(start_hz=1000, stop_hz=3000, step_hz=2000)
        arguments = ["publish_progress.py", "--repository", "owner/repo", "--platform", "ubuntu-latest",
                     "--directory", str(self.directory), "--run-id", "124", "--state", "setup",
                     "--config", str(self.config_file(configuration)), "--profile", "pilot",
                     "--software", "acousticts", "--repetitions", "1"]
        with patch.object(sys, "argv", arguments), patch.object(progress, "GitHub"), \
             patch.object(progress, "publish", return_value=True) as publish:
            progress.main()
        payload = publish.call_args.args[1]
        self.assertEqual(payload["state"], "setup")
        self.assertEqual(payload["status"]["recorded"], 0)
        self.assertEqual(payload["status"]["outcomes"], dict(pending=2))
        self.assertEqual(payload["active_software"], ["acousticts"])
        self.assertEqual(payload["retained_count"], 2)
        self.assertEqual({row["software"] for row in payload["rows"]}, {"echosms"})
        self.assertFalse((self.directory / "results.json").exists())

    def test_artifact_republication_preserves_retained_curves_and_active_counts(self):
        import benchmark
        active, sidecar = self.retained_fixture()
        sidecar["rows"][0].update(delta_ts_db=99, reference_id="stale")
        reference = dict({key: sidecar["rows"][0][key] for key in benchmark.PHYSICAL_FIELDS},
                         reference_id="d" * 64, source_sha="c" * 40, qualified=True,
                         qualification="fixture", software="acousticscattering", controls={},
                         ts_db=-32, uncertainty_db=.01)
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as output:
            output.writestr("results.json", json.dumps(active))
            output.writestr("status.json", json.dumps(dict(planned=2, recorded=0)))
            output.writestr("retained-results.json", json.dumps(sidecar))
            output.writestr("references.json", json.dumps(dict(references=[reference])))
        api = Mock(repository="owner/repo")
        api.request.side_effect = [dict(status="completed", run_attempt=1),
            dict(artifacts=[dict(id=7, name="results-ubuntu-latest")])]
        api.archive.return_value = archive.getvalue()
        payload = progress.artifact_payload(api, "ubuntu-latest", 12)
        self.assertEqual(payload["retained_count"], 2)
        self.assertEqual(payload["status"]["recorded"], 0)
        self.assertEqual(payload["status"]["outcomes"], dict(pending=2))
        self.assertEqual({row["source_sha"] for row in payload["rows"]}, {"a" * 40})
        self.assertEqual(payload["rows"][0]["delta_ts_db"], 2)
        self.assertEqual(payload["rows"][0]["reference_id"], reference["reference_id"])
        self.assertIsNone(payload["rows"][1]["delta_ts_db"])

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

    def test_snapshot_preserves_one_size_including_pending_setup(self):
        rows = [dict(semimajor_m=.01, status="ok"), dict(semimajor_m=.01, status="pending")]
        payload = progress.compact_payload("owner/repo", "ubuntu-latest", 12, 1, rows)
        self.assertEqual(payload["semimajor_m"], .01)
        self.assertEqual(payload["rows"][0]["semimajor_m"], .01)
        setup = progress.compact_payload("owner/repo", "ubuntu-latest", 12, 1, rows[1:], state="setup")
        self.assertEqual(setup["semimajor_m"], .01)
        self.assertEqual(setup["rows"], [])
        legacy = self.payload()
        self.assertIsNone(legacy["semimajor_m"])
        self.assertIsNone(legacy["rows"][0]["semimajor_m"])

    def test_snapshot_rejects_mixed_and_invalid_sizes(self):
        with self.assertRaisesRegex(ValueError, "only one semimajor"):
            progress.compact_payload("owner/repo", "ubuntu-latest", 12, 1,
                                     [dict(semimajor_m=.01), dict(semimajor_m=.07)])
        for size in (0, -1, True, "0.01", float("nan"), float("inf")):
            with self.subTest(size=size), self.assertRaisesRegex(ValueError, "finite positive"):
                progress.compact_payload("owner/repo", "ubuntu-latest", 12, 1, [dict(semimajor_m=size)])

    def test_prediction_memory_preserves_zero_and_missing_observations(self):
        payload = progress.compact_payload("owner/repo", "ubuntu-latest", 12, 1, [
            dict(status="ok", memory_delta_peak_bytes=0, memory_baseline_rss_bytes=1048576,
                 memory_peak_rss_bytes=1048576, memory_status="ok", memory_samples=2,
                 memory_method="prediction_incremental_process_tree_rss", memory_sample_interval_seconds=.01),
            dict(status="ok", memory_delta_peak_bytes=None, memory_status="unavailable")])
        self.assertEqual(payload["rows"][0]["memory_delta_peak_bytes"], 0)
        self.assertEqual(payload["rows"][0]["memory_baseline_rss_bytes"], 1048576)
        self.assertEqual(payload["rows"][0]["memory_sample_interval_seconds"], .01)
        self.assertIsNone(payload["rows"][1]["memory_delta_peak_bytes"])
        self.assertEqual(payload["rows"][1]["memory_status"], "unavailable")

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

    def test_failed_prediction_duration_survives_without_inflating_success_totals(self):
        for failure in ("numerical_error", "nonconverged"):
            with self.subTest(status=failure):
                rows = [dict(software="acousticts", boundary="gas", aspect_ratio=10,
                             incidence_deg=37, frequency_hz=frequency, repetition=1,
                             status=status, elapsed_seconds=elapsed)
                        for frequency, status, elapsed in ((1000, "ok", .4), (3000, failure, 2.0))]
                payload = progress.compact_payload("owner/repo", "ubuntu-latest", 12, 1, rows)
                failed = next(row for row in payload["rows"] if row["status"] == failure)
                self.assertEqual(failed["elapsed_seconds"], 2.0)
                self.assertEqual(failed["frequency_hz"], 3000)
                self.assertEqual(failed["repetition"], 1)
                for coverage in ("coverage", "incidence_coverage"):
                    group = payload[coverage][0]
                    self.assertEqual(group["recorded"], 2)
                    self.assertEqual(group["success"], 1)
                    self.assertEqual(group["other_recorded"], 1)
                    self.assertEqual(group["elapsed_seconds_sum"], .4)

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

    def test_angle_coverage_counts_pending_cases_without_mixing_incidences(self):
        rows = [dict(software="acousticts", boundary="gas", aspect_ratio=10,
                     incidence_deg=angle, frequency_hz=frequency, status=outcome,
                     elapsed_seconds=elapsed)
                for angle, frequency, outcome, elapsed in (
                    (0, 1000, "ok", .2), (0, 3000, "pending", None),
                    (37, 1000, "timeout", None), (90, 1000, "ok", .7))]
        payload = progress.compact_payload("owner/repo", "ubuntu-latest", 12, 1, rows)
        groups = {group["incidence_deg"]: group for group in payload["incidence_coverage"]}
        self.assertEqual(groups[0]["planned"], 2)
        self.assertEqual(groups[0]["pending"], 1)
        self.assertEqual(groups[0]["elapsed_seconds_sum"], .2)
        self.assertEqual(groups[37]["other_recorded"], 1)
        self.assertEqual(groups[90]["success"], 1)
        for field in ("planned", "recorded", "success", "other_recorded", "pending"):
            self.assertEqual(sum(group[field] for group in groups.values()), payload["coverage"][0][field])

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

    def test_latest_artifact_uses_creation_time_not_identifier_order(self):
        artifacts = [dict(id=12530932101, name="checkpoint-ubuntu-latest-1-3", created_at="2026-10-23T20:49:15Z"),
                     dict(id=12529979373, name="checkpoint-ubuntu-latest-1-5", created_at="2026-10-23T20:49:24Z")]
        self.assertEqual(progress.select_artifact(artifacts, "ubuntu-latest")["id"], 12529979373)

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
        self.assertEqual(payload["aspect_ratios"], [])
        self.assertEqual(payload["status"]["planned"], 0)
        api.archive.assert_not_called()

    def test_local_setup_uses_workflow_pilot_selection_without_observations(self):
        arguments = ["publish_progress.py", "--repository", "owner/repo", "--platform", "ubuntu-latest",
                     "--directory", str(self.directory / "results"), "--run-id", "99", "--state", "setup",
                     "--config", str(self.config_file())]
        with patch.dict(os.environ, dict(PROFILE="pilot", SOFTWARE="acousticts,echosms", REPETITIONS="3")), \
                patch.object(sys, "argv", arguments), patch.object(progress, "GitHub"), \
                patch.object(progress, "publish", return_value=True) as publish:
            progress.main()
        payload = publish.call_args.args[1]
        self.assertEqual(payload["aspect_ratios"], [10])
        self.assertEqual(payload["incidences_deg"], [90])
        self.assertEqual(payload["axes"]["10"]["frequency_hz"], [1000, 200000])
        self.assertEqual(len(payload["axes"]["10"]["frequencies_hz"]), 101)
        self.assertEqual(payload["status"]["planned"], 606)
        self.assertEqual(payload["status"]["outcomes"], dict(pending=606))
        self.assertEqual(payload["status"]["state"], "setup")
        self.assertEqual(payload["status"]["recorded"], 0)
        self.assertEqual(payload["rows"], [])
        self.assertEqual(payload["status_counts"], {})
        self.assertEqual({group["boundary"] for group in payload["coverage"]}, {"gas"})
        self.assertTrue(all(group["pending"] == 303 and group["recorded"] == 0
                            and group["elapsed_seconds_sum"] == 0 for group in payload["coverage"]))
        self.assertFalse((self.directory / "results").exists())

    def test_full_setup_plan_keeps_all_aspects_without_measurements(self):
        pending = progress.pending_setup_cases(self.config_file(), "full", "echosms", 1)
        payload = progress.compact_payload("owner/repo", "ubuntu-latest", 99, 1,
                                           pending, state="setup")
        self.assertEqual(payload["aspect_ratios"], [1.001, 1.01, 5, 10, 50, 100])
        self.assertEqual(payload["incidences_deg"], [0, 37, 90])
        self.assertEqual(payload["status"]["planned"], 6 * 3 * 4 * 101)
        self.assertEqual(len(payload["coverage"]), 6 * 4)
        self.assertEqual(payload["rows"], [])
        self.assertEqual(payload["status"]["recorded"], 0)
        self.assertTrue(all(axis["frequency_hz"] == [1000, 200000] for axis in payload["axes"].values()))

    def test_setup_plan_rejects_missing_profile_and_invalid_configuration(self):
        path = self.config_file()
        for profile in (None, "unknown"):
            with self.subTest(profile=profile), self.assertRaisesRegex(ValueError, "profile"):
                progress.pending_setup_cases(path, profile)
        invalid = self.campaign()
        invalid["frequency"]["step_hz"] = 0
        with self.assertRaisesRegex(ValueError, "step_hz"):
            progress.pending_setup_cases(self.config_file(invalid), "pilot")
        with self.assertRaisesRegex(ValueError, "configuration"):
            progress.pending_setup_cases(self.config_file({"profiles": {"pilot": {}}}), "pilot")
        with self.assertRaisesRegex(ValueError, "repetitions"):
            progress.pending_setup_cases(self.config_file(), "pilot", repetitions=0)

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

    def test_actions_requests_accept_empty_success_responses_and_dispatch_receipts(self):
        api = progress.GitHub("owner/repo", "test-token")
        api.opener = Mock()
        api.opener.open.side_effect = [io.BytesIO(b""), io.BytesIO(b'{"workflow_run_id":123}')]
        self.assertIsNone(api.request("PUT", "/actions/workflows/queue.yml/disable"))
        self.assertEqual(api.request("POST", "/actions/workflows/benchmark.yml/dispatches",
            dict(ref="main", return_run_details=True)), {"workflow_run_id": 123})

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

    def test_size_snapshot_updates_before_latest_with_separate_revision_guards(self):
        proposed = dict(self.payload(12), semimajor_m=.01)
        previous = dict(self.payload(11), semimajor_m=.07)
        api = Mock()
        api.request.side_effect = [{"ref": "exists"}, progress.APIError(404, "Missing"), {},
            {"ref": "exists"}, dict(sha="latest-sha", encoding="base64",
                content=base64.b64encode(json.dumps(previous).encode()).decode()), {}]
        self.assertTrue(progress.publish(api, proposed))
        puts = [call for call in api.request.call_args_list if call.args[0] == "PUT"]
        self.assertEqual([call.args[1] for call in puts],
                         ["/contents/ubuntu-latest-a-0.01.json", "/contents/ubuntu-latest.json"])
        self.assertNotIn("sha", puts[0].args[2])
        self.assertEqual(puts[1].args[2]["sha"], "latest-sha")
        for call in puts:
            self.assertEqual(json.loads(base64.b64decode(call.args[2]["content"])), proposed)

    def test_older_size_can_update_without_overwriting_latest_other_size(self):
        proposed = dict(self.payload(12), semimajor_m=.07)

        def content(payload, sha):
            return dict(sha=sha, encoding="base64",
                        content=base64.b64encode(json.dumps(payload).encode()).decode())

        api = Mock()
        api.request.side_effect = [{"ref": "exists"}, content(dict(self.payload(11), semimajor_m=.07), "size-sha"), {},
            {"ref": "exists"}, content(dict(self.payload(13), semimajor_m=.01), "latest-sha")]
        self.assertTrue(progress.publish(api, proposed))
        puts = [call for call in api.request.call_args_list if call.args[0] == "PUT"]
        self.assertEqual(len(puts), 1)
        self.assertEqual(puts[0].args[1], "/contents/ubuntu-latest-a-0.07.json")
        self.assertEqual(puts[0].args[2]["sha"], "size-sha")

    def test_size_conflict_preserves_newer_size_and_latest_snapshots(self):
        proposed = dict(self.payload(12), semimajor_m=.01)

        def content(run, sha):
            payload = dict(self.payload(run), semimajor_m=.01)
            return dict(sha=sha, encoding="base64",
                        content=base64.b64encode(json.dumps(payload).encode()).decode())

        api = Mock()
        api.request.side_effect = [{"ref": "exists"}, content(11, "old-size-sha"), progress.APIError(409, "Conflict"),
            {"ref": "exists"}, content(13, "new-size-sha"), {"ref": "exists"}, content(13, "latest-sha")]
        self.assertFalse(progress.publish(api, proposed))
        puts = [call for call in api.request.call_args_list if call.args[0] == "PUT"]
        self.assertEqual(len(puts), 1)
        self.assertEqual(puts[0].args[1], "/contents/ubuntu-latest-a-0.01.json")
        self.assertEqual(puts[0].args[2]["sha"], "old-size-sha")

    def test_failed_size_publication_does_not_advance_latest_snapshot(self):
        api = Mock()
        api.request.side_effect = [{"ref": "exists"}, progress.APIError(404, "Missing"),
                                   progress.APIError(500, "Failed")]
        with self.assertRaises(progress.APIError):
            progress.publish(api, dict(self.payload(), semimajor_m=.1))
        self.assertEqual(api.request.call_count, 3)
        self.assertEqual(api.request.call_args.args[1], "/contents/ubuntu-latest-a-0.1.json")

    def test_snapshot_filenames_are_limited_to_the_selected_platform_and_five_sizes(self):
        api = Mock()
        for filename in ("../ubuntu-latest.json", "windows-latest.json", "ubuntu-latest-a-0.02.json",
                         "ubuntu-latest-a-0.01.json?ref=main"):
            with self.subTest(filename=filename), self.assertRaisesRegex(ValueError, "filename"):
                progress.existing_snapshot(api, "ubuntu-latest", filename)
        api.request.assert_not_called()
        self.assertEqual([progress.snapshot_filename("ubuntu-latest", size)
                          for size in progress.SEMIMAJOR_OPTIONS],
            [f"ubuntu-latest-a-{size}.json" for size in ("0.001", "0.01", "0.07", "0.1", "0.5")])
        with self.assertRaisesRegex(ValueError, "platform"):
            progress.snapshot_filename("../ubuntu-latest")

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
