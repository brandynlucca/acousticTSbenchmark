"""Durable cache conflict handling and reference joins preserve measured data."""

import base64
import copy
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import benchmark as b
import reference_store as store
import references as refs
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


if __name__ == "__main__":
    unittest.main()
