"""Local reference scheduling, durable resume and publication failure tests."""
import copy
import json
import math
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import benchmark
import local_references as local
import reference_curves
import reference_store
import references


def configuration():
    value = benchmark.read_json(benchmark.ROOT / "benchmark.json")
    value["geometry"] = dict(semimajor_m=.01, aspect_ratios=[1.001, 1.01, 5, 10, 50, 100])
    value["frequency"] = dict(start_hz=1000, stop_hz=300000, step_hz=2000, include_stop=True)
    return value


def stage(modes):
    amplitude = complex(.01, .002)
    return dict(status="ok", ts_db=20 * math.log10(abs(amplitude)),
        amplitude=dict(real=amplitude.real, imag=amplitude.imag),
        diagnostics=dict(spatial_converged=True, solver_report=dict(converged=True,
            solver_options=dict(m_max=modes), refinement=dict(converged=True, change_db=.0001))))


def evidence(job):
    return dict(status="ok", baseline=stage(0 if job["incidence_deg"] == 0 else 20),
                expanded=None if job["incidence_deg"] == 0 else stage(28))


def jobs():
    config = configuration()
    config["geometry"]["aspect_ratios"] = [50]
    config["incidences_deg"] = [0, 37, 90]
    config["frequency"] = dict(start_hz=1000, stop_hz=1000, step_hz=2000)
    return sorted(references.reference_jobs(config, "full", "a"*40, adapters={"fixture":"b"}), key=local.priority)


def fixture(directory, selected=None):
    work = Path(directory)
    (work / "julia-smoke-rigid").mkdir()
    (work / "python-smoke").mkdir()
    location = local.paths(work)
    identity = dict(schema_version=1, repository="owner/repository", run_id=123,
        threads=4, case_timeout_seconds=600, minimum_available_bytes=local.MINIMUM_AVAILABLE,
        requests=selected or jobs(), source_guard=dict(file_sha256={}, heads={}),
        runtime=dict(source_shas={"acousticscattering":"a"*40}, environment_fingerprint="fixture"))
    manifest = dict(identity, fingerprint=benchmark.fingerprint(identity), created_at="fixture")
    benchmark.atomic_json(location["manifest"], manifest)
    benchmark.atomic_json(location["status"], dict(state="planned", manifest_fingerprint=manifest["fingerprint"],
        attempted={}, published=[], publication_errors=[], history=[]))
    return manifest, location


class LocalReferenceTests(unittest.TestCase):
    def test_literal_grid_priority_and_known_qualified_physical_skip(self):
        config = configuration()
        planned, total, skipped = local.selected_jobs(config, "full", "a"*40, [], {"fixture":"b"})
        self.assertEqual(total, 10872)
        self.assertEqual(len(planned), total)
        self.assertEqual({j["frequency_hz"] for j in planned}, {*range(1000, 300000, 2000), 300000})
        self.assertEqual({j["aspect_ratio"] for j in planned}, {1.001, 1.01, 5, 10, 50, 100})
        first = planned[:12]
        self.assertTrue(all(j["aspect_ratio"] == 50 and j["frequency_hz"] == 1000 for j in first))
        self.assertEqual({j["boundary"] for j in first}, set(benchmark.BOUNDARIES))
        self.assertEqual({j["incidence_deg"] for j in first}, {0, 37, 90})
        record = references.reference_record(first[0], evidence(first[0]))
        known = reference_curves.curves_from_records([first[0]], {first[0]["reference_id"]:record})
        remaining, _, skipped = local.selected_jobs(config, "full", "a"*40, known, {"fixture":"b"})
        self.assertEqual(len(remaining), total-1)
        self.assertEqual(skipped, [first[0]["physical_id"]])
        self.assertNotIn(first[0]["physical_id"], {j["physical_id"] for j in remaining})

    def test_os_lock_prevents_duplicate_controller(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "controller.lock"
            with local.ControllerLock(path):
                with self.assertRaisesRegex(RuntimeError, "Another"):
                    with local.ControllerLock(path):
                        self.fail("duplicate lock acquired")
            with local.ControllerLock(path):
                pass

    def test_raw_failure_and_success_are_preserved_and_resume_does_not_rerun(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest, location = fixture(directory, jobs()[:2])
            job = manifest["requests"][0]
            raw = location["stages"] / f"{job['reference_id']}.json"
            benchmark.atomic_json(raw, dict(status="timeout", phase="solving"))
            original = raw.read_bytes()
            calls = []
            def runner(todo, runtime, work, threads, deadline, timeout, accept, progress, **kwargs):
                calls.append([j["reference_id"] for j in todo])
                self.assertEqual(kwargs["attempts_dir"], location["stages"])
                self.assertEqual((threads, timeout), (4, 600))
                for request in todo:
                    accept(request, evidence(request))
                return {j["reference_id"] for j in todo}
            state = local.execute(manifest, location, runner=runner, verify=lambda _:None,
                available=lambda:local.MINIMUM_AVAILABLE, publish=lambda *args:None)
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0], [manifest["requests"][1]["reference_id"]])
            self.assertEqual(raw.read_bytes(), original)
            self.assertEqual((state["qualified"], state["failed"], state["remaining"]), (1, 1, 0))
            state = local.execute(manifest, location, resume=True,
                runner=Mock(side_effect=AssertionError("must not rerun")), verify=lambda _:None,
                available=lambda:local.MINIMUM_AVAILABLE, publish=lambda *args:None)
            self.assertEqual(state["state"], "completed")
            self.assertEqual(len(state["history"]), 2)

    def test_resource_gate_waits_without_starting_or_changing_other_work(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest, location = fixture(directory, jobs()[:1])
            samples = iter([local.MINIMUM_AVAILABLE-1, local.MINIMUM_AVAILABLE])
            waits = []
            def runner(todo, runtime, work, threads, deadline, timeout, accept, progress, **kwargs):
                self.assertEqual(waits, [15])
                accept(todo[0], evidence(todo[0]))
            state = local.execute(manifest, location, runner=runner, verify=lambda _:None,
                available=lambda:next(samples), sleep=waits.append, publish=lambda *args:None)
            self.assertEqual(state["state"], "completed")

    def test_two_zero_progress_worker_failures_stop_durably(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest, location = fixture(directory, jobs()[:1])
            runner = Mock(return_value=set())
            state = local.execute(manifest, location, runner=runner, verify=lambda _:None,
                available=lambda:local.MINIMUM_AVAILABLE, publish=lambda *args:None)
            self.assertEqual(runner.call_count, 2)
            self.assertEqual(state["state"], "stopped_after_worker_failures")
            self.assertEqual(state["remaining"], 1)
            self.assertEqual(benchmark.read_json(location["status"])["consecutive_worker_failures"], 2)

    def test_recorded_worker_errors_advance_then_stop_after_two(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest, location = fixture(directory, jobs()[:3])
            seen = []
            def runner(todo, runtime, work, threads, deadline, timeout, accept, progress, **kwargs):
                seen.append(todo[0]["reference_id"])
                accept(todo[0], dict(status="worker_error", message="fixture worker exit"))
            state = local.execute(manifest, location, runner=runner, verify=lambda _:None,
                available=lambda:local.MINIMUM_AVAILABLE, publish=lambda *args:None)
            self.assertEqual(len(set(seen)), 2)
            self.assertEqual(state["remaining"], 1)
            self.assertEqual(state["failed"], 2)
            self.assertEqual(state["state"], "stopped_after_worker_failures")

    def test_publication_failure_keeps_results_and_resume_retries_without_solving(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest, location = fixture(directory, jobs()[:1])
            def runner(todo, runtime, work, threads, deadline, timeout, accept, progress, **kwargs):
                accept(todo[0], evidence(todo[0]))
            failed = Mock(side_effect=RuntimeError("do not persist credential details"))
            state = local.execute(manifest, location, runner=runner, verify=lambda _:None,
                available=lambda:local.MINIMUM_AVAILABLE, publish=failed)
            self.assertEqual(state["state"], "completed_publication_pending")
            self.assertEqual(state["publication_pending"], 1)
            self.assertEqual(state["publication_errors"][0]["error"], "RuntimeError")
            uploaded = []
            def publish(plan, selected, cache):
                uploaded.extend(j["reference_id"] for j in selected)
            state = local.execute(manifest, location, resume=True,
                runner=Mock(side_effect=AssertionError("must not solve again")), verify=lambda _:None,
                available=lambda:local.MINIMUM_AVAILABLE, publish=publish)
            self.assertEqual(state["state"], "completed")
            self.assertEqual(state["publication_pending"], 0)
            self.assertEqual(uploaded, [manifest["requests"][0]["reference_id"]])

    def test_stop_file_pauses_before_start_and_after_saved_case(self):
        for initial in (True, False):
            with self.subTest(initial=initial), tempfile.TemporaryDirectory() as directory:
                manifest, location = fixture(directory, jobs()[:2])
                if initial:
                    location["stop"].write_text("user pause")
                def runner(todo, runtime, work, threads, deadline, timeout, accept, progress, **kwargs):
                    self.assertFalse(initial)
                    location["stop"].write_text("user pause")
                    accept(todo[0], evidence(todo[0]))
                    self.fail("continued after pause")
                state = local.execute(manifest, location, runner=runner, verify=lambda _:None,
                    available=lambda:local.MINIMUM_AVAILABLE, publish=lambda *args:None)
                self.assertEqual(state["state"], "paused")
                self.assertEqual(state["attempted_count"], 0 if initial else 1)

    def test_source_guard_failure_prevents_worker_start(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest, location = fixture(directory, jobs()[:1])
            runner = Mock()
            with self.assertRaisesRegex(ValueError, "changed"):
                local.execute(manifest, location, runner=runner,
                    verify=Mock(side_effect=ValueError("sources changed")), publish=lambda *args:None)
            runner.assert_not_called()

    def test_publication_flush_is_bounded_and_all_cases_remain_distinct(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest, location = fixture(directory)
            batches = []
            def runner(todo, runtime, work, threads, deadline, timeout, accept, progress, **kwargs):
                for job in todo:
                    accept(job, evidence(job))
            def publish(plan, selected, cache):
                batches.append([job["reference_id"] for job in selected])
                self.assertLessEqual(len(selected), 10)
            state = local.execute(manifest, location, runner=runner, verify=lambda _:None,
                available=lambda:local.MINIMUM_AVAILABLE, publish=publish)
            self.assertEqual(state["qualified"], 12)
            self.assertEqual(len({key for batch in batches for key in batch}), 12)
            self.assertEqual(state["publication_pending"], 0)

    def test_publication_uses_separate_cache_branch_and_fresh_api_deadlines(self):
        manifest = dict(repository="owner/repository", run_id=123)
        factory = Mock(side_effect=[object(), object(), object()])
        with patch.object(reference_curves, "curves_from_cache", return_value=["curve"]), \
             patch.object(reference_store, "publish_cache") as cache, \
             patch.object(reference_curves, "publish_shared_curves", create=True) as shared, \
             patch.object(reference_curves, "publish_curves") as run:
            local.publish_records(manifest, ["job"], Path("cache"), api_factory=factory)
        self.assertEqual(factory.call_count, 3)
        self.assertEqual(cache.call_args.kwargs, {"branch":"local-reference-data"})
        self.assertEqual(shared.call_args.args[1], ["curve"])
        self.assertEqual(run.call_args.args[1:], (123, ["curve"]))


if __name__ == "__main__":
    unittest.main()
