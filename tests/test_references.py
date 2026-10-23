"""Reference identity, independent qualification, and cache resume tests."""

import copy
import math
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import benchmark as b
import references as r


def stage(modes, amplitude=complex(0.01, 0.002), change=0.0001):
    return dict(status="ok", ts_db=20 * math.log10(abs(amplitude)),
                amplitude=dict(real=amplitude.real, imag=amplitude.imag),
                diagnostics=dict(spatial_converged=True, solver_report=dict(converged=True,
                    solver_options=dict(m_max=modes), refinement=dict(converged=True, change_db=change))))


def evidence():
    return dict(status="ok", baseline=stage(20), expanded=stage(28, complex(0.010001, 0.002)))


class ReferenceTests(unittest.TestCase):
    def setUp(self):
        self.config = b.read_json(ROOT / "benchmark.json")
        self.lock = b.read_json(ROOT / "sources.lock.json")
        self.jobs = r.build_requests(self.config, self.lock, "pilot")
        self.job = self.jobs[0]

    def test_identity_excludes_modal_versions_but_tracks_physics_solver_and_adapter(self):
        changed = copy.deepcopy(self.lock)
        changed["sources"]["acousticts"]["sha"] = "a" * 40
        self.assertEqual(self.job["reference_id"], r.build_requests(self.config, changed, "pilot")[0]["reference_id"])
        config = copy.deepcopy(self.config)
        config["repetitions"] = 3
        config["software"] = ["echosms"]
        self.assertEqual(self.jobs, r.build_requests(config, self.lock, "pilot"))
        for field, value in (("semimajor_m", 0.08),):
            changed = copy.deepcopy(self.config)
            changed["geometry"][field] = value
            self.assertNotEqual(self.job["reference_id"], r.build_requests(changed, self.lock, "pilot")[0]["reference_id"])
        changed = copy.deepcopy(self.config)
        changed["controls"]["acousticscattering"]["gas"]["order"] = 8
        self.assertNotEqual(self.job["reference_id"], r.build_requests(changed, self.lock, "pilot")[0]["reference_id"])
        altered = r.reference_jobs(self.config, "pilot", self.job["source_sha"], adapters={"worker_reference.jl": "changed"})
        self.assertNotEqual(self.job["reference_id"], altered[0]["reference_id"])

    def test_checked_reference_loads_and_joins_with_correct_sign(self):
        record = r.reference_record(self.job, evidence())
        self.assertTrue(record["qualified"])
        self.assertTrue(r.valid_cached(record, self.job))
        self.assertEqual(record["uncertainty_db"], 0.01)
        self.assertIn("not an absolute error bound", record["qualification"]["criterion"])
        self.assertEqual(record["ts_db"], evidence()["expanded"]["ts_db"])
        case = b.build_cases(self.config, "pilot", ["echosms"])[0]
        rows = b.summarize([case], {case["case_id"]: dict(status="ok", ts_db=record["ts_db"] + 2)},
                           {record["physical_id"]: record})
        self.assertAlmostEqual(rows[0]["delta_ts_db"], 2)

    def test_spatial_or_fourier_failure_never_becomes_qualified(self):
        scenarios = []
        invalid = evidence()
        invalid["expanded"]["diagnostics"]["solver_report"]["refinement"]["change_db"] = 0.02
        scenarios.append(invalid)
        invalid = evidence()
        invalid["expanded"] = stage(28, complex(0.011, 0.002))
        scenarios.append(invalid)
        invalid = evidence()
        invalid["expanded"] = stage(20)
        scenarios.append(invalid)
        invalid = evidence()
        invalid["expanded"] = stage(28, complex(-0.01, -0.002))
        scenarios.append(invalid)  # Same TS but incorrect complex phase.
        scenarios.append(dict(status="timeout", qualified=True))
        for invalid in scenarios:
            with self.subTest(invalid=invalid):
                record = r.reference_record(self.job, invalid)
                self.assertFalse(record["qualified"])
                self.assertIsNone(record["uncertainty_db"])
                self.assertFalse(r.valid_cached(record, self.job))

    def test_axial_symmetry_requires_m_zero(self):
        config = copy.deepcopy(self.config)
        config["profiles"]["pilot"]["incidences_deg"] = [0]
        job = r.build_requests(config, self.lock, "pilot")[0]
        record = r.reference_record(job, dict(status="ok", baseline=stage(0), expanded=None))
        self.assertTrue(record["qualified"])
        self.assertEqual(record["uncertainty_db"], 0.005)
        self.assertFalse(r.reference_record(job, dict(status="ok", baseline=stage(20)))["qualified"])

    def test_tampered_payload_or_key_is_rejected(self):
        record = r.reference_record(self.job, evidence())
        for field, value in (("ts_db", -99), ("semimajor_m", 0.1), ("source_sha", "0" * 40),
                             ("controls", {}), ("qualified", False), ("reference_id", "a" * 64),
                             ("physical_id", "a" * 64), ("schema_version", 7), ("status", "nonconverged")):
            altered = copy.deepcopy(record)
            altered[field] = value
            self.assertFalse(r.valid_cached(altered, self.job), field)
        altered = copy.deepcopy(record)
        altered["evidence"]["expanded"]["diagnostics"]["spatial_converged"] = False
        self.assertFalse(r.valid_cached(altered, self.job))

    def test_resume_computes_only_missing_and_export_only_needs_no_runtime(self):
        scratch = ROOT / ".scratch/codex/dense-benchmark/tests"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as temporary:
            directory = Path(temporary)
            self.config["frequency"] = dict(start_hz=1000, stop_hz=3000, step_hz=2000)
            b.atomic_json(directory / "config.json", self.config)
            jobs = r.build_requests(self.config, self.lock, "pilot")
            cache = directory / "cache"
            b.atomic_json(cache / (jobs[0]["reference_id"] + ".json"), r.reference_record(jobs[0], evidence()))
            runtime = dict(source_shas={"acousticscattering": self.job["source_sha"]},
                           availability={"acousticscattering": {"status": "ready"}}, environment_fingerprint="test")
            b.atomic_json(directory / "runtime.json", runtime)
            args = r.parser().parse_args(["--config", str(directory / "config.json"),
                "--cache-dir", str(cache), "--output", str(directory / "references.json"),
                "--runtime", str(directory / "runtime.json")])

            def finish(missing, runtime, work, threads, deadline, timeout, accept, progress):
                self.assertEqual([j["reference_id"] for j in missing], [jobs[1]["reference_id"]])
                accept(missing[0], evidence())
                return {missing[0]["reference_id"]}

            with patch.object(r, "_run_batch", side_effect=finish) as run_batch:
                self.assertEqual(r.run(args), 0)
                args.runtime = None
                self.assertEqual(r.run(args), 0)
                self.assertEqual(run_batch.call_count, 1)
            self.assertEqual(len(b.load_references(directory / "references.json")), 2)
            args.export_only, args.runtime = True, None
            self.assertEqual(r.run(args), 0)
            self.assertEqual(b.read_json(directory / "reference-status.json")["qualified"], 2)

    def test_plan_contains_full_keys_without_fabricated_observations(self):
        scratch = ROOT / ".scratch/codex/dense-benchmark/tests"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as temporary:
            directory = Path(temporary)
            args = r.parser().parse_args(["--plan", "--cache-dir", str(directory / "cache"),
                                          "--output", str(directory / "plan.json")])
            self.assertEqual(r.run(args), 0)
            plan = b.read_json(directory / "plan.json")
            self.assertEqual(plan["planned"], 101)
            self.assertNotIn("ts_db", plan["requests"][0])
            self.assertEqual(plan["requests"][0]["signature"]["controls"]["mode_threads"], 4)
            self.assertFalse((directory / "cache").exists())

    def test_batch_collects_final_atomic_result_written_during_process_exit(self):
        scratch = ROOT / ".scratch/codex/dense-benchmark/tests"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as temporary:
            directory = Path(temporary)
            process = MagicMock(returncode=0)

            def exit_with_result():
                b.atomic_json(directory / "reference-attempts" / (self.job["reference_id"] + ".json"), evidence())
                return 0

            process.poll.side_effect = exit_with_result
            runtime = dict(executables={"acousticscattering": ["julia", "--threads=1"]})
            accepted = MagicMock()
            with patch.object(r.subprocess, "Popen", return_value=process), patch.object(r, "_stop") as stop, \
                 patch("process_control.WindowsJob"):
                completed = r._run_batch([self.job], runtime, directory, 4, 1e20, 600, accepted, MagicMock())
            self.assertEqual(completed, {self.job["reference_id"]})
            accepted.assert_called_once()
            self.assertEqual(accepted.call_args.args[1], evidence())
            stop.assert_called_once()

    def test_batch_timeout_records_failure_and_terminates_owned_process_once(self):
        scratch = ROOT / ".scratch/codex/dense-benchmark/tests"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as temporary:
            directory = Path(temporary)
            process = MagicMock(returncode=0)
            process.poll.return_value = None
            runtime = dict(executables={"acousticscattering": ["julia", "--threads=1"]})
            accepted = MagicMock()
            with patch.object(r.subprocess, "Popen", return_value=process), patch.object(r, "_stop") as stop, \
                 patch("process_control.WindowsJob"), patch.object(r.time, "time", side_effect=[100, 101, 101, 101]):
                completed = r._run_batch([self.job], runtime, directory, 4, 1000, 0.5, accepted, MagicMock())
            stop.assert_called_once()
            self.assertEqual(completed, {self.job["reference_id"]})
            self.assertEqual(accepted.call_args.args[1]["status"], "timeout")
            self.assertFalse(r.reference_record(self.job, accepted.call_args.args[1])["qualified"])

    def test_source_signature_ignores_checkout_line_endings(self):
        scratch = ROOT / ".scratch/codex/dense-benchmark/tests"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as temporary:
            path = Path(temporary) / "adapter.txt"
            path.write_bytes(b"line one\nline two\n")
            first = r.source_digest(path)
            path.write_bytes(b"line one\r\nline two\r\n")
            self.assertEqual(first, r.source_digest(path))
            path.write_bytes(b"changed\nline two\n")
            self.assertNotEqual(first, r.source_digest(path))

    def test_compact_provenance_retains_package_versions_without_machine_paths(self):
        runtime = dict(source_shas={"acousticscattering": self.job["source_sha"]}, environment_compatibility=dict(
            julia_environments=dict(acousticscattering={"Manifest.toml": dict(julia_version="1.12.1", deps={
                "AcousticScattering": [dict(uuid="source-id", version="0.1.1", path="C:/private/source")],
                "Ferrite": [dict(uuid="ferrite-id", version="1.7.0", **{"git-tree-sha1": "a" * 40}, deps=["ignored-edges"])]}),
                "LocalPreferences.toml": "[AcousticScattering]\nprecompile_workload = false\n"})))
        result = r.julia_provenance(runtime)
        self.assertEqual(result["julia_version"], "1.12.1")
        self.assertEqual(result["packages"]["Ferrite"][0]["git-tree-sha1"], "a" * 40)
        self.assertEqual(result["packages"]["AcousticScattering"][0]["version"], "0.1.1")
        self.assertNotIn("path", result["packages"]["AcousticScattering"][0])
        self.assertNotIn("deps", result["packages"]["Ferrite"][0])
        self.assertFalse(result["preferences"]["AcousticScattering"]["precompile_workload"])


if __name__ == "__main__":
    unittest.main()
