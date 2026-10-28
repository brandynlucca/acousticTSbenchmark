"""Durable cache conflict handling and reference joins preserve measured data."""

import base64
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import textwrap
import unittest
import zipfile
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import benchmark as b
import reference_store as store
import references as refs
import reference_curves as curves
from publish_progress import APIError


def evidence():
    def stage(modes):
        return dict(status="ok", ts_db=-40.0, amplitude=dict(real=.01, imag=0),
                    diagnostics=dict(spatial_converged=True, solver_report=dict(
                        converged=True, refinement=dict(converged=True, change_db=.001),
                        solver_options=dict(m_max=modes))))
    return dict(status="ok", baseline=stage(20), expanded=stage(28))


class MemoryGit:
    repository = "owner/repo"

    def __init__(self):
        self.head = None
        self.trees = {}
        self.commits = {}
        self.blobs = {}
        self.conflict_once = False
        self.injected = None

    def repository_archive(self, head):
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            tree = self.trees[self.commits[head]["tree"]["sha"]]
            for path, sha in tree.items():
                archive.writestr("owner-repo-commit/" + path, self.blobs[sha])
        return output.getvalue()

    def request(self, method, path, body=None):
        if method == "GET" and path.startswith("/git/ref/"):
            if self.head is None:
                raise APIError(404, "absent")
            return dict(object=dict(sha=self.head))
        if method == "GET" and path.startswith("/git/commits/"):
            return self.commits[path.rsplit("/", 1)[1]]
        if method == "GET" and path.startswith("/git/trees/"):
            tree = self.trees[path.rsplit("/", 1)[1].split("?")[0]]
            return dict(tree=[dict(path=p, sha=s, type="blob") for p, s in tree.items()])
        if method == "GET" and path.startswith("/git/blobs/"):
            raw = self.blobs[path.rsplit("/", 1)[1]]
            return dict(encoding="base64", content=base64.b64encode(raw).decode(), size=len(raw))
        if method == "POST" and path == "/git/trees":
            tree = dict(self.trees.get(body.get("base_tree"), {}))
            for entry in body["tree"]:
                raw = entry["content"].encode()
                sha = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
                self.blobs[sha] = raw
                tree[entry["path"]] = sha
            sha = f"tree-{len(self.trees)}"
            self.trees[sha] = tree
            return dict(sha=sha)
        if method == "POST" and path == "/git/commits":
            sha = f"commit-{len(self.commits)}"
            self.commits[sha] = dict(tree=dict(sha=body["tree"]), parents=body["parents"])
            return dict(sha=sha)
        if method in ("POST", "PATCH") and path.startswith("/git/refs"):
            if self.conflict_once:
                self.conflict_once = False
                self.injected()
                raise APIError(422, "concurrent commit")
            if method == "PATCH":
                assert body["force"] is False
                assert self.head in self.commits[body["sha"]]["parents"]
            self.head = body["sha"]
            return {}
        raise AssertionError((method, path, body))


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        self.config = b.read_json(b.ROOT / "benchmark.json")
        self.config["frequency"] = dict(start_hz=1000, stop_hz=3000, step_hz=2000)
        self.lock = b.read_json(b.ROOT / "sources.lock.json")
        self.jobs = refs.build_requests(self.config, self.lock, "pilot")
        self.records = [refs.reference_record(job, evidence()) for job in self.jobs]
        self.api = MemoryGit()

    def save(self, index, directory="cache"):
        b.atomic_json(self.work / directory / (self.jobs[index]["reference_id"] + ".json"), self.records[index])

    def test_restore_only_verified_keys_and_skip_existing(self):
        self.save(0)
        self.assertEqual(store.publish_cache(self.api, self.jobs, self.work / "cache"), 1)
        restored = store.restore_cache(self.api, self.jobs, self.work / "restored")
        self.assertEqual(restored, dict(planned=2, cached=1, missing=1))
        self.assertEqual(store.publish_cache(self.api, self.jobs, self.work / "restored"), 0)
        with patch.object(store, "blob_record", side_effect=AssertionError("must use local cache")):
            self.assertEqual(store.restore_cache(self.api, self.jobs, self.work / "restored")["cached"], 1)

    def test_local_reference_cache_uses_a_separate_branch(self):
        self.save(0)
        with patch.object(self.api, "request", wraps=self.api.request) as request:
            self.assertEqual(store.publish_cache(self.api, self.jobs, self.work / "cache",
                                                branch="local-reference-data"), 1)
        paths = [call.args[1] for call in request.call_args_list]
        self.assertIn("/git/ref/heads/local-reference-data", paths)
        self.assertNotIn("/git/ref/heads/reference-data", paths)
        created = next(call for call in request.call_args_list
                       if call.args[:2] == ("POST", "/git/refs"))
        self.assertEqual(created.args[2]["ref"], "refs/heads/local-reference-data")
        with self.assertRaisesRegex(ValueError, "Unsupported"):
            store.publish_cache(self.api, self.jobs, self.work / "cache", branch="main")

    def test_simultaneous_first_publish_preserves_both_shards(self):
        self.save(0)
        self.save(1, "second")
        self.api.conflict_once = True
        self.api.injected = lambda: store.publish_cache(self.api, self.jobs, self.work / "second")
        self.assertEqual(store.publish_cache(self.api, self.jobs, self.work / "cache"), 1)
        self.assertEqual(store.restore_cache(self.api, self.jobs, self.work / "restored")["cached"], 2)

    def test_unqualified_and_tampered_records_are_not_published(self):
        self.records[0]["qualified"] = False
        self.records[1]["ts_db"] += 3
        self.save(0)
        self.save(1)
        self.assertEqual(store.publish_cache(self.api, self.jobs, self.work / "cache"), 0)
        self.assertIsNone(self.api.head)

    def test_dense_cache_restore_uses_one_archive_instead_of_per_case_api_reads(self):
        self.config["frequency"]["stop_hz"] = 19000
        jobs = refs.build_requests(self.config, self.lock, "pilot")
        for job in jobs:
            b.atomic_json(self.work / "cache" / (job["reference_id"] + ".json"),
                          refs.reference_record(job, evidence()))
        self.assertEqual(store.publish_cache(self.api, jobs, self.work / "cache"), 10)
        with patch.object(store, "blob_record", side_effect=AssertionError("use archive")):
            result = store.restore_cache(self.api, jobs, self.work / "restored")
        self.assertEqual(result, dict(planned=10, cached=10, missing=0))

    def test_join_keeps_timings_and_pending_cases_while_clearing_stale_comparisons(self):
        cases = b.build_cases(self.config, software=["echosms"])
        measurements = {cases[0]["case_id"]: dict(status="ok", elapsed_seconds=.123, ts_db=-39.0)}
        b.export(self.work, cases, measurements, {}, "budget_paused")
        path = self.work / "selected.json"
        b.atomic_json(path, dict(references=[self.records[0]]))
        with patch.object(b, "measure", side_effect=AssertionError("join cannot measure")):
            self.assertEqual(store.join_directory(self.work, path), 1)
        rows = b.read_json(self.work / "results.json")
        self.assertEqual((rows[0]["elapsed_seconds"], rows[0]["delta_ts_db"]), (.123, 1.0))
        self.assertEqual(rows[1]["status"], "pending")
        self.assertEqual(b.read_json(self.work / "status.json")["recorded"], 1)
        b.atomic_json(path, dict(references=[]))
        store.join_directory(self.work, path)
        row = b.read_json(self.work / "results.json")[0]
        self.assertIsNone(row["delta_ts_db"])
        self.assertNotIn("reference_ts_db", row)

    def test_setup_only_artifact_has_no_comparisons_but_partial_data_still_errors(self):
        def archive(extra=None):
            output = io.BytesIO()
            with zipfile.ZipFile(output, "w") as z:
                z.writestr("benchmark-source.json", json.dumps(dict(schema_version=1,
                    repository="owner/repo", source_ref="a" * 40, run_id=123)))
                for name, value in (extra or {}).items():
                    z.writestr(name, json.dumps(value))
            return output.getvalue()
        api = Mock(repository="owner/repo")
        api.request.return_value = dict(artifacts=[dict(id=1, name="results-macos-latest")])
        api.archive.return_value = archive()
        self.assertFalse(store.download_timing(api, 123, "macos-latest", self.work))
        self.assertFalse((self.work / "manifest.json").exists())
        api.archive.return_value = archive({"results.json": []})
        with self.assertRaisesRegex(ValueError, "Incomplete timing artifact"):
            store.download_timing(api, 123, "macos-latest", self.work)

    def test_reference_join_preserves_retained_measurements_and_active_counts(self):
        cases = b.build_cases(self.config, software=["echosms"])
        retained_case = b.build_cases(self.config, software=["prol_spheroid"])[0]
        retained = dict(retained_case, status="ok", elapsed_seconds=.456, ts_db=-38,
                        source_sha="b" * 40, measurement_role="retained", retained_from_run_id=100,
                        delta_ts_db=99, reference_ts_db=-137, reference_id="stale")
        sidecar = self.work / "retained-results.json"
        b.atomic_json(sidecar, dict(schema_version=1, rows=[retained],
                                   provenance=[dict(run_id=100, manifest_fingerprint="c" * 64)]))
        original = sidecar.read_bytes()
        measurements = {cases[0]["case_id"]: dict(status="ok", elapsed_seconds=.123, ts_db=-39)}
        b.export(self.work, cases, measurements, {}, "budget_paused")
        path = self.work / "selected.json"
        b.atomic_json(path, dict(references=[self.records[0]]))
        with patch.object(b, "measure", side_effect=AssertionError("retained comparisons cannot measure")):
            self.assertEqual(store.join_directory(self.work, path), 2)
        active = b.read_json(self.work / "results.json")
        self.assertEqual({row["software"] for row in active}, {"echosms"})
        self.assertEqual(b.read_json(self.work / "status.json")["recorded"], 1)
        self.assertEqual(b.read_json(self.work / "status.json")["planned"], 2)
        combined = store.comparison_rows(self.work, active)
        saved = next(row for row in combined if row.get("measurement_role") == "retained")
        self.assertEqual((saved["elapsed_seconds"], saved["delta_ts_db"]), (.456, 2))
        self.assertEqual(saved["source_sha"], "b" * 40)
        self.assertEqual(saved["retained_from_run_id"], 100)
        self.assertEqual(saved["reference_id"], self.records[0]["reference_id"])
        self.assertEqual(sidecar.read_bytes(), original)
        self.assertIn('"measurement_role": "retained"', (self.work / "progress.html").read_text(encoding="utf-8"))
        b.atomic_json(path, dict(references=[]))
        store.join_directory(self.work, path)
        saved = store.comparison_rows(self.work, b.read_json(self.work / "results.json"))[-1]
        self.assertIsNone(saved["delta_ts_db"])
        self.assertNotIn("reference_ts_db", saved)
        self.assertEqual(sidecar.read_bytes(), original)

    def test_refresh_baseline_without_timing_manifest_skips_reference_attachment(self):
        import refresh
        cases = b.build_cases(self.config, software=["echosms"])
        retained = dict(schema_version=1, rows=[], provenance=[])
        metadata = dict(schema_version=1, repository="owner/repo", run_id=123, platform="ubuntu-latest",
                        retained_fingerprint=b.fingerprint(retained), sources=self.lock,
                        settings=dict(configuration=self.config, profile="pilot", software="echosms",
                                      repetitions=1, source_ref="a" * 40, planned_cases=len(cases),
                                      sources_fingerprint=b.fingerprint(self.lock)))
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as output:
            output.writestr("refresh-campaign.json", json.dumps(metadata))
            output.writestr("retained-results.json", json.dumps(retained))
        self.assertEqual(refresh.baseline_settings(archive.getvalue(), "owner/repo", 123, "ubuntu-latest")["recorded_cases"], 0)
        api = Mock(repository="owner/repo")
        api.request.return_value = dict(artifacts=[dict(id=1, name="results-ubuntu-latest")])
        api.archive.return_value = archive.getvalue()
        self.assertFalse(store.download_timing(api, 123, "ubuntu-latest", self.work))
        self.assertFalse((self.work / "manifest.json").exists())

    def test_download_and_attach_retain_other_software_with_updated_references(self):
        cases = b.build_cases(self.config, software=["echosms"])
        rows = b.summarize(cases, {}, {})
        retained_case = b.build_cases(self.config, software=["prol_spheroid"])[0]
        retained = dict(retained_case, status="ok", elapsed_seconds=.456, ts_db=-38,
                        source_sha="b" * 40, measurement_role="retained", retained_from_run_id=100)
        sidecar = dict(schema_version=1, rows=[retained], provenance=[dict(run_id=100)])
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            for name, value in {"results.json": rows, "status.json": dict(state="budget_paused", planned=2, recorded=0),
                                "manifest.json": dict(reference_requests=self.jobs),
                                "retained-results.json": sidecar}.items():
                archive.writestr(name, json.dumps(value))
        api = Mock(repository="owner/repo")
        api.request.side_effect = [dict(artifacts=[dict(id=1, name="results-ubuntu-latest")]),
                                   dict(run_attempt=1, status="in_progress")]
        api.archive.return_value = output.getvalue()
        self.save(0)
        directory = self.work / "attached"
        arguments = ["reference_store.py", "attach", "--repository", "owner/repo", "--run-id", "123",
                     "--platform", "ubuntu-latest", "--directory", str(directory),
                     "--cache-dir", str(self.work / "cache"), "--output", str(self.work / "references.json")]
        with patch.object(sys, "argv", arguments), patch.object(store, "GitHub", return_value=api), \
             patch.object(store, "restore_cache", return_value=dict(planned=2, cached=1, missing=1)), \
             patch("publish_progress.publish") as publish, patch.object(curves, "publish_curves") as overlay:
            store.main()
        self.assertEqual(overlay.call_args.args[1], 123)
        self.assertEqual(len(overlay.call_args.args[2]), 1)
        self.assertEqual(b.read_json(directory / "retained-results.json"), sidecar)
        payload = publish.call_args.args[1]
        self.assertEqual(payload["state"], "running")
        self.assertEqual(payload["retained_count"], 1)
        self.assertEqual(payload["status"]["recorded"], 0)
        self.assertEqual(payload["status"]["outcomes"], dict(pending=2))
        self.assertEqual(payload["rows"][0]["delta_ts_db"], 2)
        self.assertEqual(payload["rows"][0]["source_sha"], "b" * 40)

    def test_reference_curves_exist_before_any_software_measurement(self):
        from publish_progress import compact_payload
        cases = b.build_cases(self.config, software=["acousticts", "echosms"], repetitions=3)
        reference_map = b.references_from_value(dict(references=self.records))
        rows = b.summarize(cases, {}, reference_map)
        payload = compact_payload("owner/repo", "ubuntu-latest", 123, 1, rows)
        self.assertEqual(payload["rows"], [])
        self.assertEqual(payload["status"]["recorded"], 0)
        self.assertEqual(payload["status"]["outcomes"], dict(pending=len(cases)))
        self.assertEqual(len(payload["reference_curves"]), 2)
        self.assertEqual({r["ts_db"] for r in payload["reference_curves"]}, {-40})
        self.assertEqual({r["method"] for r in payload["reference_curves"]}, {"fem_bem"})
        self.assertEqual(curves.curves_from_rows(b.summarize(cases, {}, {})), [])

    def test_reference_curve_cache_rechecks_convergence_and_exact_inputs(self):
        records = {r["reference_id"]: copy.deepcopy(r) for r in self.records}
        records[self.jobs[0]["reference_id"]]["evidence"]["baseline"]["diagnostics"]["spatial_converged"] = False
        points = curves.curves_from_records(self.jobs, records)
        self.assertEqual(len(points), 1)
        self.assertEqual(points[0]["reference_id"], self.jobs[1]["reference_id"])
        expected = b.build_cases(self.config, software=["echosms"])[1]
        for field in ("semimajor_m", "aspect_ratio", "frequency_hz", "k0a", "h0"):
            self.assertEqual(points[0][field], expected[field])

    def test_reference_curve_publication_merges_shards_without_touching_timings(self):
        points = curves.curves_from_records(self.jobs, {r["reference_id"]: r for r in self.records})
        saved = {}
        calls = []
        def request(method, path, body=None):
            calls.append((method, path))
            self.assertTrue(path.startswith("/contents/reference-curves-123.json"))
            if method == "GET":
                if not saved:
                    raise APIError(404, "missing")
                return dict(sha="existing", encoding="base64", content=saved["content"])
            self.assertEqual(method, "PUT")
            saved.update(body)
            return {}
        api = Mock()
        api.request.side_effect = request
        self.assertEqual(curves.publish_curves(api, 123, points[:1]), 1)
        self.assertEqual(curves.publish_curves(api, 123, points[1:]), 1)
        count = len(calls)
        self.assertEqual(curves.publish_curves(api, 123, points), 0)
        self.assertEqual(len(calls), count + 1)
        payload = json.loads(base64.b64decode(saved["content"]))
        self.assertEqual(payload["run_id"], 123)
        self.assertEqual(len(payload["reference_curves"]), 2)
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            curves.publish_curves(api, 123, [dict(points[0], ts_db=-30)])

    def test_shared_qualified_curves_preserve_existing_cases_across_runs(self):
        points = curves.curves_from_records(self.jobs, {r["reference_id"]: r for r in self.records})
        saved = {}
        def request(method, path, body=None):
            self.assertTrue(path.startswith("/contents/qualified-reference-curves.json"))
            if method == "GET":
                if not saved:
                    raise APIError(404, "missing")
                return dict(sha="existing", encoding="base64", content=saved["content"])
            self.assertEqual(method, "PUT")
            self.assertEqual(body["branch"], "benchmark-data")
            saved.update(body)
            return {}
        api = Mock()
        api.request.side_effect = request
        self.assertEqual(curves.publish_shared_curves(api, points[:1]), 1)
        self.assertEqual(curves.publish_shared_curves(api, points[1:]), 1)
        self.assertEqual(curves.publish_shared_curves(api, points), 0)
        payload = json.loads(base64.b64decode(saved["content"]))
        self.assertIsNone(payload["run_id"])
        self.assertEqual(len(payload["reference_curves"]), 2)
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            curves.publish_shared_curves(api, [dict(points[0], ts_db=-30)])

    def test_live_controller_attaches_new_references_without_remeasuring(self):
        self.save(0)
        store.publish_cache(self.api, self.jobs, self.work / "cache")
        directory = self.work / "benchmark-results"
        cases = b.build_cases(self.config, software=["echosms"])
        measurements = {cases[0]["case_id"]: dict(status="ok", elapsed_seconds=.123,
                                                memory_delta_peak_bytes=4096, ts_db=-39.0)}
        b.export(directory, cases, measurements, {}, "budget_paused")
        b.atomic_json(directory / "manifest.json", dict(reference_requests=self.jobs))
        configuration = self.work / "configuration.json"
        b.atomic_json(configuration, self.config)
        selected = self.work / "references.json"
        b.atomic_json(selected, dict(references=[]))
        workflow = (b.ROOT / ".github/workflows/benchmark.yml").read_text()
        block = workflow.split("      - name: Prepare bounded timing controller\n", 1)[1]
        source = block.split("<<'PY'\n", 1)[1].split("\n          PY", 1)[0]
        namespace = {"__name__": "reference_controller_fixture"}
        exec(compile(textwrap.dedent(source), "reference-controller", "exec"), namespace)
        environment = dict(RUNNER_TEMP=str(self.work), BENCHMARK_CONFIG=str(configuration),
            BENCHMARK_REFERENCES=str(selected), BENCHMARK_DEADLINE="0", PROFILE="pilot",
            SOFTWARE="echosms", REPETITIONS="1", GITHUB_REPOSITORY="owner/repo",
            GITHUB_TOKEN="fixture", BENCHMARK_PLATFORM="ubuntu-latest",
            GITHUB_RUN_ID="123", GITHUB_RUN_ATTEMPT="1")
        published = []
        with patch.dict(os.environ, environment), patch.object(sys, "argv", ["controller", "30"]), \
             patch.object(namespace["subprocess"], "run", side_effect=AssertionError("must not measure")), \
             patch.dict(namespace, GitHub=lambda *a, **k: self.api,
                        publish=lambda api, payload: published.append(payload)):
            namespace["main"]()
        rows = b.read_json(directory / "results.json")
        self.assertEqual(rows[0]["elapsed_seconds"], .123)
        self.assertEqual(rows[0]["memory_delta_peak_bytes"], 4096)
        self.assertEqual(rows[0]["delta_ts_db"], 1.0)
        self.assertEqual(rows[1]["status"], "pending")
        self.assertEqual(published[0]["rows"][0]["delta_ts_db"], 1.0)
        self.assertEqual(published[0]["semimajor_m"], self.config["geometry"]["semimajor_m"])
        self.assertEqual(published[0]["rows"][0]["semimajor_m"], self.config["geometry"]["semimajor_m"])
        self.assertEqual(len(b.read_json(selected)["references"]), 1)


if __name__ == "__main__":
    unittest.main()
