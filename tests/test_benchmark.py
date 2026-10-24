"""Tests for scientific case identity, resumability and bounded execution."""

import copy
import io
import json
import math
import os
from pathlib import Path
import sys
import tempfile
import textwrap
import threading
import unittest
from contextlib import redirect_stderr
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import benchmark as b
import process_control


class CaseTests(unittest.TestCase):
    def setUp(self):
        self.config = b.read_json(ROOT / "benchmark.json")

    def test_dense_grid_and_fixed_major_axis(self):
        grid = b.frequencies(self.config)
        self.assertEqual((grid[0], grid[-1], len(grid)), (1000, 300000, 151))
        self.assertTrue(all(y - x == 2000 for x, y in zip(grid[:-2], grid[1:-1])))
        cases = b.build_cases(self.config, "full", ["acousticts"])
        self.assertEqual(len(cases), 4 * 6 * 3 * 151)
        self.assertEqual({c["semimajor_m"] for c in cases}, {0.07})
        for case in cases:
            self.assertAlmostEqual(case["semimajor_m"] / case["semiminor_m"], case["aspect_ratio"])
            self.assertAlmostEqual(case["k0a"], 2 * math.pi * case["frequency_hz"] * 0.07 / 1477.3)
            if case["boundary"] == "gas":
                self.assertAlmostEqual(case["hs"] / case["h0"], 1477.3 / 345)
            if case["boundary"] == "rigid":
                self.assertIsNone(case["hs"])

    def test_case_identity_tracks_physics_and_controls_separately(self):
        case = b.build_cases(self.config, software=["prol_spheroid"])[0]
        self.config["controls"][case["software"]]["linear_maxiter"] += 1
        changed = b.build_cases(self.config, software=["prol_spheroid"])[0]
        self.assertNotEqual(case["case_id"], changed["case_id"])
        self.assertEqual(case["physical_id"], changed["physical_id"])
        self.config["medium"]["sound_speed_m_s"] += 1
        different = b.build_cases(self.config, software=["prol_spheroid"])[0]
        self.assertNotEqual(changed["physical_id"], different["physical_id"])

    def test_invalid_geometry_and_duplicate_selection(self):
        self.config["profiles"]["pilot"]["aspect_ratios"] = [1]
        with self.assertRaises(ValueError):
            b.build_cases(self.config)
        self.config["profiles"]["pilot"]["aspect_ratios"] = [10, 10]
        with self.assertRaises(ValueError):
            b.build_cases(self.config)

    def test_acousticscattering_is_reference_only(self):
        self.assertNotIn("acousticscattering", self.config["software"])
        args = b.parser().parse_args(["--software", "acousticscattering", "--plan"])
        with self.assertRaisesRegex(ValueError, "reference-only"):
            b.run(args)

    def test_balanced_software_order_keeps_each_spectrum_dense(self):
        cases = b.build_cases(self.config)
        count = len(self.config["software"])
        self.assertNotEqual(cases[0]["software"], cases[count]["software"])
        for software in self.config["software"]:
            self.assertEqual([c["frequency_hz"] for c in cases if c["software"] == software],
                             b.frequencies(self.config))

    def test_round_robin_visits_all_boundaries_angles_and_ratios_before_next_frequency(self):
        self.config["frequency"] = dict(start_hz=1000, stop_hz=3000, step_hz=2000)
        legacy = b.build_cases(self.config, "full")
        self.config["case_order"] = "frequency_round_robin"
        cases = b.build_cases(self.config, "full")
        self.assertEqual({c["case_id"] for c in cases}, {c["case_id"] for c in legacy})
        first_wave = [c for c in cases if c["frequency_hz"] == 1000]
        self.assertEqual(cases[:len(first_wave)], first_wave)
        first_group = cases[:len(b.BOUNDARIES)*len(self.config["software"])]
        self.assertEqual({c["boundary"] for c in first_group}, set(b.BOUNDARIES))
        self.assertEqual({c["incidence_deg"] for c in first_wave}, {0, 37, 90})
        self.assertEqual({c["aspect_ratio"] for c in first_wave}, set(self.config["geometry"]["aspect_ratios"]))
        for case in first_wave:
            series = [c["frequency_hz"] for c in cases if all(c[k] == case[k] for k in
                ("software", "boundary", "aspect_ratio", "incidence_deg", "repetition"))]
            self.assertEqual(series, [1000, 3000])


class FileTests(unittest.TestCase):
    def setUp(self):
        root = ROOT / ".scratch/codex/dense-benchmark/tests"
        root.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=root)
        self.path = Path(self.temporary.name)
        self.config = b.read_json(ROOT / "benchmark.json")
        self.case = b.build_cases(self.config, software=["echosms"])[0]

    def tearDown(self):
        self.temporary.cleanup()

    def test_timeout_kills_worker_and_preserves_censoring(self):
        self.case["controls"] = {"behavior": "sleep"}
        runtime = {"executables": {"echosms": [sys.executable]}}
        fixture = ROOT / "tests/worker_fixture.py"
        with patch.object(b, "command_for", side_effect=lambda c, r, q, s: [sys.executable, str(fixture), str(q), str(s)]):
            # Windows process-gate and interpreter startup can exceed 3 s.
            # Allow startup to finish so this tests a timeout during the solve.
            result = b.measure(self.case, runtime, self.path / "worker", 10)
        self.assertEqual(result["status"], "timeout")
        self.assertIsNone(result["elapsed_seconds"])
        self.assertGreater(result["solve_lower_bound_seconds"], 0)
        self.assertLess(result["wall_seconds"], 15)

    def test_setup_timeout_is_not_a_solve_time(self):
        self.case["controls"] = {"behavior": "startup_sleep"}
        fixture = ROOT / "tests/worker_fixture.py"
        with patch.object(b, "command_for", side_effect=lambda c, r, q, s: [sys.executable, str(fixture), str(q), str(s)]):
            result = b.measure(self.case, {}, self.path / "worker", 3)
        self.assertEqual(result["status"], "setup_timeout")
        self.assertIsNone(result["solve_lower_bound_seconds"])
        self.assertLess(result["wall_seconds"], 8)

    def test_resume_retains_completed_cases_and_rejects_changed_manifest(self):
        self.config["frequency"] = dict(start_hz=1000, stop_hz=3000, step_hz=1000)
        self.config["case_timeout_seconds"] = 0.1
        config_path = self.path / "configuration.json"
        b.atomic_json(config_path, self.config)
        sha = b.read_json(ROOT / "sources.lock.json")["sources"]["echosms"]["sha"]
        runtime_path = self.path / "runtime.json"
        b.atomic_json(runtime_path, dict(source_shas={"echosms": sha}, environment_fingerprint="test"))
        args = b.parser().parse_args(["--config", str(config_path), "--software", "echosms",
            "--runtime", str(runtime_path), "--output", str(self.path / "run"), "--max-cases", "1"])
        with patch.object(b, "measure", return_value=dict(status="ok", elapsed_seconds=0.2, ts_db=-40)) as measure:
            b.run(args)
            b.run(args)
            self.assertEqual(measure.call_count, 2)
            cases = b.read_json(self.path / "run/results.json")
            self.assertEqual([row["status"] for row in cases], ["ok", "ok", "pending"])
            args.max_cases = None
            b.run(args)
            b.run(args)
            self.assertEqual(measure.call_count, 3)
            changed_observer = dict(b.memory_provenance(), psutil_version="different-observer-version")
            with patch.object(b, "memory_provenance", return_value=changed_observer):
                with self.assertRaisesRegex(ValueError, "Resume refused"):
                    b.run(args)
            self.assertEqual(measure.call_count, 3)
            self.config["geometry"]["semimajor_m"] = 0.08
            b.atomic_json(config_path, self.config)
            with self.assertRaisesRegex(ValueError, "Resume refused"):
                b.run(args)

    def test_reference_matching_sign_and_self_comparison(self):
        reference = {key: self.case[key] for key in b.PHYSICAL_FIELDS}
        reference.update(qualified=True, reference_id="independent-ref", source_sha="a" * 40,
                         qualification={"evidence": "independent convergence study"},
                         uncertainty_db=0.01, ts_db=-41, software="acousticscattering",
                         controls={"boundary_order": 9})
        path = self.path / "references.json"
        b.atomic_json(path, {"references": [reference]})
        refs = b.load_references(path)
        records = {self.case["case_id"]: dict(status="ok", ts_db=-40, source_sha="b" * 40)}
        row = b.summarize([self.case], records, refs)[0]
        self.assertEqual(row["delta_ts_db"], 1)
        reference["software"] = self.case["software"]
        reference["source_sha"] = "b" * 40
        reference["controls"] = self.case["controls"]
        b.atomic_json(path, {"references": [reference]})
        self.assertIsNone(b.summarize([self.case], records, b.load_references(path))[0]["delta_ts_db"])
        reference["qualified"] = False
        b.atomic_json(path, {"references": [reference]})
        self.assertEqual(b.load_references(path), {})

    def test_missing_references_and_failures_do_not_become_zero_error(self):
        record = {self.case["case_id"]: dict(status="numerical_error", ts_db=None)}
        row = b.summarize([self.case], record, {})[0]
        self.assertIsNone(row["delta_ts_db"])
        self.assertEqual(row["status"], "numerical_error")

    def test_checkpoint_survives_export_error(self):
        record = self.path / "case.json"
        b.atomic_json(record, {"status": "ok", "elapsed_seconds": 1.0})
        with self.assertRaises(ValueError):
            b.atomic_json(record, {"elapsed_seconds": float("nan")})
        self.assertEqual(b.read_json(record)["elapsed_seconds"], 1.0)

    def test_publication_credentials_never_reach_solver_environment(self):
        runtime = dict(executables={"echosms": ["fixture"]},
                       env={"GITHUB_TOKEN": "runtime-secret", "GH_TOKEN": "runtime-gh-secret",
                            "BENCHMARK_CUSTOM": "retained"})

        def inspect(command, **kwargs):
            self.assertNotIn("GITHUB_TOKEN", kwargs["env"])
            self.assertNotIn("GH_TOKEN", kwargs["env"])
            self.assertEqual(kwargs["env"]["BENCHMARK_CUSTOM"], "retained")
            raise OSError("fixture stops before launching a process")

        with patch.dict(os.environ, {"GITHUB_TOKEN": "parent-secret", "GH_TOKEN": "parent-gh-secret"}), \
             patch.object(b.subprocess, "Popen", side_effect=inspect) as launch:
            result = b.measure(self.case, runtime, self.path / "worker", 10)
        self.assertEqual(result["status"], "setup_error")
        launch.assert_called_once()

    def memory_fixture(self, behavior, timeout=8):
        self.case["controls"] = {"behavior": behavior}
        fixture = ROOT / "tests/worker_fixture.py"
        with patch.object(b, "command_for", side_effect=lambda c, r, q, s: [sys.executable, str(fixture), str(q), str(s)]):
            return b.measure(self.case, {}, self.path / "worker", timeout)

    @unittest.skipIf(process_control.psutil is None, "psutil is not installed")
    def test_prediction_memory_excludes_warmup_and_postprocessing_even_for_fast_calls(self):
        result = self.memory_fixture("memory_fast")
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(result["memory_status"], "ok", result)
        self.assertGreater(result["memory_baseline_rss_bytes"], 32 * 1024 * 1024)
        self.assertGreater(result["memory_delta_peak_bytes"], 24 * 1024 * 1024)
        self.assertLess(result["memory_delta_peak_bytes"], 80 * 1024 * 1024)
        self.assertEqual(result["memory_delta_peak_bytes"],
                         result["memory_peak_rss_bytes"] - result["memory_baseline_rss_bytes"])
        self.assertGreaterEqual(result["memory_samples"], 1)
        self.assertEqual(result["memory_method"], "prediction_incremental_process_tree_rss")
        self.assertEqual(result["memory_sample_interval_seconds"], .01)
        self.assertFalse(any(t.name == "prediction-memory" for t in threading.enumerate()))

    @unittest.skipIf(process_control.psutil is None, "psutil is not installed")
    def test_prediction_memory_records_peak_of_allocation_freed_before_return(self):
        result = self.memory_fixture("memory_release")
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(result["memory_status"], "ok", result)
        self.assertGreater(result["memory_delta_peak_bytes"], 24 * 1024 * 1024)
        self.assertLess(result["memory_delta_peak_bytes"], 80 * 1024 * 1024)

    @unittest.skipIf(process_control.psutil is None, "psutil is not installed")
    def test_prediction_memory_includes_native_style_child_and_timeout_is_censored(self):
        result = self.memory_fixture("memory_child")
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(result["memory_status"], "ok", result)
        self.assertGreater(result["memory_delta_peak_bytes"], 40 * 1024 * 1024)
        # Allow for three interpreter startups on Windows before testing the
        # actual timeout. Production deadlines are independent of this fixture.
        result = self.memory_fixture("memory_child_timeout", timeout=10)
        self.assertEqual(result["status"], "timeout", result)
        self.assertEqual(result["memory_status"], "censored", result)
        self.assertIsNone(result["elapsed_seconds"])
        self.assertGreater(result["memory_delta_peak_bytes"], 40 * 1024 * 1024)
        for pid in b.read_json(self.path / "worker/child-pids.json"):
            try:
                process = process_control.psutil.Process(pid)
                status = process.status()
            except process_control.psutil.NoSuchProcess:
                continue
            self.assertEqual(status, process_control.psutil.STATUS_ZOMBIE)
        self.assertFalse(any(t.name == "prediction-memory" for t in threading.enumerate()))

    def test_missing_or_failed_memory_sampler_never_fails_the_prediction(self):
        with patch.object(process_control, "psutil", None):
            result = self.memory_fixture("memory_fast")
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(result["memory_status"], "unavailable")
        self.assertIsNone(result["memory_delta_peak_bytes"])
        if process_control.psutil is not None:
            with patch.object(process_control.PredictionMemory, "_rss", side_effect=OSError("fixture access denied")):
                result = self.memory_fixture("memory_fast")
            self.assertEqual(result["status"], "ok", result)
            self.assertEqual(result["memory_status"], "sampling_error", result)
            self.assertIsNone(result["memory_delta_peak_bytes"])
            self.assertIsNone(result["memory_peak_rss_bytes"])
        self.assertFalse(any(t.name == "prediction-memory" for t in threading.enumerate()))


class ProgressTests(unittest.TestCase):
    def setUp(self):
        root = ROOT / ".scratch/codex/dense-benchmark/tests"
        root.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=root)
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name)
        self.environment = dict(GITHUB_TOKEN="secret-for-publication", GITHUB_REPOSITORY="owner/repo",
                                BENCHMARK_PLATFORM="ubuntu-latest", GITHUB_RUN_ID="123", GITHUB_RUN_ATTEMPT="2")

    def arguments(self):
        config = b.read_json(ROOT / "benchmark.json")
        config["frequency"] = dict(start_hz=1000, stop_hz=5000, step_hz=1000)
        config["case_timeout_seconds"] = 1
        b.atomic_json(self.path / "config.json", config)
        sha = b.read_json(ROOT / "sources.lock.json")["sources"]["echosms"]["sha"]
        b.atomic_json(self.path / "runtime.json", dict(source_shas={"echosms": sha}, environment_fingerprint="test"))
        return b.parser().parse_args(["--config", str(self.path / "config.json"), "--software", "echosms",
            "--runtime", str(self.path / "runtime.json"), "--output", str(self.path / "run"), "--publish-progress"])

    def test_publish_initial_first_and_rate_limited_results_only_between_workers(self):
        import publish_progress

        args = self.arguments()
        clock, active, measurements, published, events = [0.0], [False], [], [], []
        finishes = iter((1, 10, 30, 32, 40))

        def measure(case, runtime, directory, timeout):
            active[0] = True
            events.append(("measure", len(measurements) + 1))
            clock[0] = next(finishes)
            measurements.append(case["case_id"])
            active[0] = False
            return dict(status="ok", elapsed_seconds=0.1, ts_db=-40)

        def publish(api, payload):
            self.assertFalse(active[0], "Network publication must never overlap a timed worker")
            count = payload["status"]["recorded"]
            self.assertEqual(count, len(measurements))
            self.assertEqual(len(payload["rows"]), count)
            self.assertEqual(payload["state"], "running")
            published.append((count, clock[0]))
            events.append(("publish", count))

        with patch.dict(os.environ, self.environment), patch.object(b.time, "monotonic", side_effect=lambda: clock[0]), \
             patch.object(b, "measure", side_effect=measure), patch.object(publish_progress, "GitHub") as api, \
             patch.object(publish_progress, "publish", side_effect=publish):
            self.assertEqual(b.run(args), 0)
        self.assertEqual(published, [(0, 0), (1, 1), (4, 32), (5, 40)])
        self.assertEqual(events[:3], [("publish", 0), ("measure", 1), ("publish", 1)])
        self.assertEqual(api.call_count, 4)
        self.assertTrue(all(call.kwargs["timeout_seconds"] == 15 for call in api.call_args_list))
        self.assertEqual(b.read_json(self.path / "run/manifest.json")["workload"],
                         "fresh_process_warmed_distinct_problem_single_prediction")

    def test_failure_backoff_preserves_measurements_and_redacts_error_details(self):
        import publish_progress

        args = self.arguments()
        clock, attempts, count = [0.0], [], [0]
        finishes = iter((1, 10, 29, 31, 32))

        def measure(*args):
            clock[0] = next(finishes)
            count[0] += 1
            return dict(status="ok", elapsed_seconds=0.1, ts_db=-40)

        def fail(api, payload):
            attempts.append((payload["status"]["recorded"], clock[0]))
            raise RuntimeError("secret-for-publication and private signed URL")

        errors = io.StringIO()
        with patch.dict(os.environ, self.environment), patch.object(b.time, "monotonic", side_effect=lambda: clock[0]), \
             patch.object(b, "measure", side_effect=measure), patch.object(publish_progress, "GitHub"), \
             patch.object(publish_progress, "publish", side_effect=fail), redirect_stderr(errors):
            self.assertEqual(b.run(args), 0)
        self.assertEqual(attempts, [(0, 0), (4, 31), (5, 32)])  # Final checkpoint is always attempted.
        self.assertEqual(count[0], 5)
        self.assertEqual(b.read_json(self.path / "run/status.json")["recorded"], 5)
        self.assertNotIn("secret-for-publication", errors.getvalue())
        self.assertNotIn("private signed URL", errors.getvalue())
        self.assertIn("retrying after 30 seconds", errors.getvalue())

    def test_publication_is_opt_in(self):
        import publish_progress

        args = self.arguments()
        args.publish_progress = False
        with patch.dict(os.environ, self.environment), \
             patch.object(b, "measure", return_value=dict(status="ok", elapsed_seconds=0.1, ts_db=-40)), \
             patch.object(publish_progress, "GitHub") as api:
            self.assertEqual(b.run(args), 0)
        api.assert_not_called()


class CampaignWorkflowTests(unittest.TestCase):
    @staticmethod
    def script(step):
        workflow = (ROOT / ".github/workflows/benchmark.yml").read_text()
        block = workflow.split(f"      - name: {step}\n", 1)[1].split("\n      - ", 1)[0]
        source = block.split("<<'PY'\n", 1)[1].split("\n          PY", 1)[0]
        return textwrap.dedent(source)

    def setUp(self):
        root = ROOT / ".scratch/codex/dense-benchmark/tests"
        root.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=root)
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name)
        self.directory = self.path / "benchmark-results"
        self.directory.mkdir()
        b.atomic_json(self.directory / "status.json", dict(state="budget_paused", recorded=123))
        b.atomic_json(self.directory / "results.json", [])
        self.namespace = {"__name__": "workflow_controller_fixture"}
        exec(compile(self.script("Prepare bounded timing controller"), "workflow-controller", "exec"), self.namespace)
        self.environment = dict(RUNNER_TEMP=str(self.path), BENCHMARK_DEADLINE="3700",
            GITHUB_REPOSITORY="owner/repo", GITHUB_TOKEN="publication-only", GH_TOKEN="also-private",
            BENCHMARK_PLATFORM="ubuntu-latest", GITHUB_RUN_ID="456", GITHUB_RUN_ATTEMPT="1",
            PROFILE="pilot", SOFTWARE="echosms", REPETITIONS="1")
        self.environment["BENCHMARK_CONFIG"] = str(ROOT / "benchmark.json")

    def execute(self, advance, duration=2700):
        clock, commands, published = [0.0], [], []

        def run(command, **kwargs):
            self.assertTrue(kwargs["check"])
            self.assertNotIn("GITHUB_TOKEN", kwargs["env"])
            self.assertNotIn("GH_TOKEN", kwargs["env"])
            self.assertNotIn("--publish-progress", command)
            self.assertLessEqual(float(command[command.index("--chunk-seconds") + 1]), 30)
            commands.append(command)
            clock[0] += 90
            status = b.read_json(self.directory / "status.json")
            status["recorded"] += advance
            b.atomic_json(self.directory / "status.json", status)

        def payload(*args):
            self.assertEqual(args[-1], "running")
            return args[-2]

        with patch.dict(os.environ, self.environment), patch.object(sys, "argv", ["controller", str(duration)]), \
             patch.object(self.namespace["time"], "monotonic", side_effect=lambda: clock[0]), \
             patch.object(self.namespace["time"], "time", side_effect=lambda: 1000 + clock[0]), \
             patch.object(self.namespace["subprocess"], "run", side_effect=run), \
             patch.dict(self.namespace, GitHub=lambda *a, **k: None, compact_payload=payload,
                        publish=lambda api, data: published.append(data["recorded"])):
            self.namespace["main"]()
        return clock[0], commands, published

    def test_continuation_preserves_count_and_uses_remaining_budget_after_five_minutes(self):
        elapsed, commands, published = self.execute(1)
        self.assertGreater(elapsed, 300)
        self.assertLess(elapsed, 2700)
        self.assertEqual(published[0], 123)
        self.assertEqual(published[-1], 123 + len(commands))
        self.assertIn("--max-cases", commands[0])
        self.assertTrue(all("--max-cases" not in command for command in commands[1:]))
        self.assertEqual(b.read_json(self.directory / "status.json")["recorded"], published[-1])

    def test_no_progress_or_expired_deadline_does_not_loop(self):
        _, commands, published = self.execute(0)
        self.assertEqual(len(commands), 1)
        self.assertEqual(set(published), {123})
        self.environment["BENCHMARK_DEADLINE"] = "1000"
        _, commands, _ = self.execute(1)
        self.assertEqual(commands, [])

    def test_selected_budget_can_continue_beyond_the_original_45_minutes(self):
        self.environment.update(BENCHMARK_BUDGET_SECONDS="7200", BENCHMARK_DEADLINE="8200")
        elapsed, commands, _ = self.execute(1, duration=7200)
        self.assertGreater(elapsed, 2700)
        self.assertLess(elapsed, 7200)
        self.assertTrue(all(command[command.index("--budget-seconds") + 1] == "7200" for command in commands))

    def test_worker_failure_propagates_after_final_publication(self):
        import subprocess
        with patch.dict(os.environ, self.environment), patch.object(sys, "argv", ["controller", "2700"]), \
             patch.object(self.namespace["time"], "time", return_value=1000), \
             patch.object(self.namespace["subprocess"], "run", side_effect=subprocess.CalledProcessError(2, "worker")), \
             patch.dict(self.namespace, GitHub=lambda *a, **k: None, compact_payload=lambda *a: a[-2]), \
             patch.dict(self.namespace, publish=lambda api, data: None):
            with self.assertRaises(subprocess.CalledProcessError):
                self.namespace["main"]()
        self.assertEqual(b.read_json(self.directory / "status.json")["recorded"], 123)

if __name__ == "__main__":
    unittest.main()
