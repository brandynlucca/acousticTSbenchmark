"""Tests for scientific case identity, resumability and bounded execution."""

import copy
import io
import json
import math
import os
from pathlib import Path
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import benchmark as b


class CaseTests(unittest.TestCase):
    def setUp(self):
        self.config = b.read_json(ROOT / "benchmark.json")

    def test_dense_grid_and_fixed_major_axis(self):
        grid = b.frequencies(self.config)
        self.assertEqual((grid[0], grid[-1], len(grid)), (1000, 200000, 101))
        self.assertTrue(all(y - x == 2000 for x, y in zip(grid[:-2], grid[1:-1])))
        cases = b.build_cases(self.config, "full", ["acousticts"])
        self.assertEqual(len(cases), 4 * 6 * 3 * 101)
        self.assertEqual({c["semimajor_m"] for c in cases}, {0.07})
        for case in cases:
            self.assertAlmostEqual(case["semimajor_m"] / case["semiminor_m"], case["aspect_ratio"])
            self.assertAlmostEqual(case["k0a"], 2 * math.pi * case["frequency_hz"] * 0.07 / 1477.3)
            if case["boundary"] == "gas":
                self.assertAlmostEqual(case["hs"] / case["h0"], 1477.3 / 345)
            if case["boundary"] == "rigid":
                self.assertIsNone(case["hs"])

    def test_case_identity_tracks_physics_and_controls_separately(self):
        case = b.build_cases(self.config)[0]
        self.config["controls"][case["software"]]["linear_maxiter"] += 1
        changed = b.build_cases(self.config)[0]
        self.assertNotEqual(case["case_id"], changed["case_id"])
        self.assertEqual(case["physical_id"], changed["physical_id"])
        self.config["medium"]["sound_speed_m_s"] += 1
        different = b.build_cases(self.config)[0]
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
            result = b.measure(self.case, runtime, self.path / "worker", 3)
        self.assertEqual(result["status"], "timeout")
        self.assertIsNone(result["elapsed_seconds"])
        self.assertGreater(result["solve_lower_bound_seconds"], 0)
        self.assertLess(result["wall_seconds"], 8)

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


if __name__ == "__main__":
    unittest.main()
