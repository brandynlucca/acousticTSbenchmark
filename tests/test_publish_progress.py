"""Public snapshot content, artifact choice, and concurrent publication guards."""

import base64
import copy
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import publish_progress as progress


class GitAPI:
    """Small in-memory Git object store with an atomic branch reference."""

    repository = "owner/repo"

    def __init__(self, snapshots=None, *, empty=False):
        self.blobs, self.trees, self.commits = {}, {}, {}
        self.head = None
        self.counter = 0
        self.conflict = None
        self.ref_error = None
        self.request = Mock(side_effect=self._request)
        if not empty:
            self.install(snapshots or {})

    def object_id(self):
        self.counter += 1
        return f"{self.counter:040x}"

    def add_blob(self, value):
        raw = json.dumps(value, separators=(",", ":")).encode()
        sha = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
        self.blobs[sha] = raw
        return sha

    def install(self, snapshots):
        tree = dict(self.trees[self.commits[self.head]["tree"]]) if self.head else {}
        tree.update({name: self.add_blob(value) for name, value in snapshots.items()})
        tree_id, commit_id = self.object_id(), self.object_id()
        self.trees[tree_id] = tree
        self.commits[commit_id] = {"tree": tree_id, "parents": [self.head] if self.head else []}
        self.head = commit_id

    def current(self, name):
        tree = self.trees[self.commits[self.head]["tree"]]
        return json.loads(self.blobs[tree[name]]) if name in tree else None

    def _request(self, method, path, body=None):
        if method == "GET" and path == "/git/ref/heads/benchmark-data":
            if self.head is None:
                raise progress.APIError(404, "Missing")
            return {"object": {"sha": self.head}}
        if method == "GET" and path.startswith("/git/commits/"):
            return {"tree": {"sha": self.commits[path.rsplit("/", 1)[-1]]["tree"]}}
        if method == "GET" and path.startswith("/git/trees/"):
            tree = self.trees[path.rsplit("/", 1)[-1]]
            return {"tree": [dict(path=name, type="blob", sha=sha, size=len(self.blobs[sha])) for name, sha in tree.items()]}
        if method == "GET" and path.startswith("/git/blobs/"):
            sha = path.rsplit("/", 1)[-1]
            return dict(sha=sha, encoding="base64", size=len(self.blobs[sha]), content=base64.b64encode(self.blobs[sha]).decode())
        if method == "POST" and path == "/git/blobs":
            raw = base64.b64decode(body["content"])
            sha = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
            self.blobs[sha] = raw
            return {"sha": sha}
        if method == "POST" and path == "/git/trees":
            tree = dict(self.trees.get(body.get("base_tree"), {}))
            tree.update({entry["path"]: entry["sha"] for entry in body["tree"]})
            sha = self.object_id()
            self.trees[sha] = tree
            return {"sha": sha}
        if method == "POST" and path == "/git/commits":
            sha = self.object_id()
            self.commits[sha] = {"tree": body["tree"], "parents": body["parents"]}
            return {"sha": sha}
        if (method, path) in (("PATCH", "/git/refs/heads/benchmark-data"), ("POST", "/git/refs")):
            if self.ref_error:
                raise self.ref_error
            if self.conflict is not None:
                changes, self.conflict = self.conflict, None
                self.install(changes)
                raise progress.APIError(409, "Conflict")
            if method == "PATCH":
                if body.get("force") is not False or self.commits[body["sha"]]["parents"] != [self.head]:
                    raise AssertionError("Publication must advance the current branch without force")
            elif self.head is not None:
                raise progress.APIError(422, "Already exists")
            self.head = body["sha"]
            return {"object": {"sha": self.head}}
        raise AssertionError(f"Unexpected Git operation: {method} {path}")


class ProgressTests(unittest.TestCase):
    def test_per_software_frequency_plan_keeps_pending_points(self):
        rows = [dict(software=software, frequency_hz=frequency, aspect_ratio=10,
                     incidence_deg=90, boundary="gas", status=status, ts_db=-30)
                for software, frequency, status in [
                    ("acousticts", 1000, "ok"), ("acousticts", 3000, "ok"),
                    ("liquid_spheroid", 2000, "ok"), ("liquid_spheroid", 4000, "pending"),
                    ("liquid_spheroid", 6000, "nonconverged")]]
        payload = progress.compact_payload("owner/repo", "ubuntu-latest", 12, 1, rows)
        self.assertEqual(payload["axes"]["10"]["software_frequencies_hz"],
                         {"acousticts": [1000, 3000], "liquid_spheroid": [2000, 4000, 6000]})
        self.assertEqual(payload["axes"]["10"]["frequencies_hz"], [1000, 2000, 3000, 4000, 6000])
        self.assertEqual(len(payload["rows"]), 4)

    @unittest.skipUnless(shutil.which("node"), "Node.js is needed to exercise the plotted segments")
    def test_interleaved_frequency_curves_keep_real_missing_point_gaps(self):
        source = (progress.ROOT / "src/report.py").read_text(encoding="utf-8")
        start = source.index("   for(const software of Object.keys(colors))for(const angle of selectedAngles){")
        end = source.index("   for(const row of timedFailures", start)
        loop = source[start:end]
        harness = r"""
const colors={acousticts:'black',liquid_spheroid:'blue'}, labels=colors;
const selectedAngles=[90], aspect={value:'10'}, boundary='gas', measure='ts_db';
const allFreq=[1000,2000,3000,4000,5000,6000];
const data={axes:{'10':{software_frequencies_hz:{acousticts:[1000,3000,5000],liquid_spheroid:[2000,4000,6000]}}}};
const grouped=new Map(), axisValues=new Map(allFreq.map(f=>[f,f]));
for(const [software,frequencies] of [['acousticts',[1000,3000,5000]],['liquid_spheroid',[2000,6000]]])
 for(const frequency of frequencies)grouped.set(JSON.stringify([boundary,software,90,frequency]),[{ts_db:1}]);
const segments=[],svg={append:n=>segments.push(n.attrs)},svgElement=(tag,attrs)=>({tag,attrs});
const validValue=(r,m)=>Number.isFinite(r[m]),valueFor=(r,m)=>r[m],median=v=>v[0];
const x=v=>v,y=v=>v,transform=v=>v,formatted=v=>String(v),point=()=>{},angleStyle=()=>({dash:''});
let count=0;
""" + loop + "console.log(JSON.stringify({segments,count}));"
        result = subprocess.run([shutil.which("node"), "-e", harness], check=True,
                                capture_output=True, text=True)
        result = json.loads(result.stdout)
        self.assertEqual(result["count"], 5)
        self.assertEqual([segment["d"] for segment in result["segments"]],
                         ["M 1000 1 L 3000 1", "M 3000 1 L 5000 1"])
        self.assertEqual({segment["stroke"] for segment in result["segments"]}, {"black"})

    @unittest.skipUnless(shutil.which("node"), "Node.js is needed to exercise dashboard calculations")
    def test_dashboard_totals_follow_each_grid_and_references_match_exact_cases(self):
        source = (progress.ROOT / "src/report.py").read_text(encoding="utf-8")
        helpers = source[source.index("function referenceCurves(){"):source.index("let preferredSize=")]
        helpers += source[source.index("function progressGroups(byAngle){"):source.index("function progressBar(")]
        harness = r"""
const assert=require('node:assert/strict');
const repository='owner/repo',colors={acousticts:'blue',liquid_spheroid:'brown'};
const aspect={value:'10'},incidence={value:'all'},matchesSize=()=>true,supportsBoundary=()=>true;
const old={software:'acousticts',aspect_ratio:10,boundary:'gas',planned:151,success:151,other_recorded:0};
const fresh={software:'liquid_spheroid',aspect_ratio:10,boundary:'gas',planned:50,success:2,other_recorded:1};
let data={coverage:[old,fresh],incidence_coverage:[{...old,incidence_deg:37},{...fresh,incidence_deg:37}]};
let rows=[{...old,measurement_role:'retained',status:'ok'}];
let localReferenceCurves=[];
let externalReferenceCurves=[{physical_id:'exact',reference_id:'qualified',ts_db:-30,semimajor_m:.01,aspect_ratio:10,incidence_deg:37,frequency_hz:1000,uncertainty_db:.001}];
""" + helpers + r"""
assert.deepEqual(benchmarkProgress().map(r=>[r.software,r.recorded,r.planned,r.pending]),[['acousticts',151,151,0],['liquid_spheroid',3,50,47]]);
incidence.value='37';assert.equal(benchmarkProgress(true,'gas')[1].pending,47);
incidence.value='90';assert.equal(benchmarkProgress(true,'gas')[0].planned,0);
const original=[{physical_id:'exact',status:'ok',ts_db:-29.5},{physical_id:'missing',status:'ok',ts_db:-20},{physical_id:'exact',status:'pending',ts_db:null},{physical_id:'exact',status:'ok',ts_db:-29,reference_id:'saved',reference_ts_db:-30,delta_ts_db:1}];
localReferenceCurves=[{...externalReferenceCurves[0],ts_db:-40,origin:'local_manuscript'},{...externalReferenceCurves[0],physical_id:'local-only',reference_id:'local',origin:'local_manuscript'}];
assert.equal(referenceCurves().length,2);assert.equal(referenceCurves().find(r=>r.physical_id==='exact').ts_db,-30);
assert.equal(matchedReferenceRows([{physical_id:'local-only',status:'ok',ts_db:-29}])[0].delta_ts_db,1);
const joined=matchedReferenceRows(original);
assert.equal(joined[0].delta_ts_db,.5);assert.equal(joined[0].reference_id,'qualified');
assert.equal(joined[1].reference_id,undefined);assert.equal(joined[2].delta_ts_db,null);
assert.equal(joined[3],original[3]);assert.equal(original[0].reference_id,undefined);
"""
        subprocess.run([shutil.which("node"), "-e", harness], check=True, capture_output=True, text=True)

    def setUp(self):
        progress._SNAPSHOT_GUARDS.clear()
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

    def setup_baseline_fixture(self, software="acousticts"):
        import benchmark
        active, sidecar = self.retained_fixture()
        config = self.campaign()
        config["frequency"] = dict(start_hz=1000, stop_hz=3000, step_hz=2000)
        config["controls"].setdefault(software, {})
        active = [dict(case, status="pending") for case in benchmark.build_cases(config, "pilot", [software], 1)]
        sources = dict(sources={name: dict(sha="a" * 40) for name in benchmark.CAPABILITIES})
        original_cases = benchmark.build_cases(config, "pilot", [software, "echosms"], 1)
        identity = dict(configuration=config, sources=sources, profile="pilot",
            cases=[row["case_id"] for row in original_cases], adapter_hashes=benchmark.adapter_hashes(),
            platform="Linux", architecture="x86_64", environment_fingerprint="b" * 64,
            workload="fresh_process_warmed_distinct_problem_single_prediction")
        environment = dict(platform="Linux", architecture="x86_64", threads=1,
            runtime=dict(source_shas={software: "a" * 40},
                         environment_compatibility=dict(r_packages=dict(R="4.5.2"),
                             python_packages="numpy==2.2.6\nscipy==1.15.3\npandas==2.3.0\nmatplotlib==3.10.3\n",
                             prol_compiler="GNU Fortran 14.2.0", prol_flags=["-O2"])))
        environment_id = benchmark.fingerprint(environment)
        sidecar["provenance"] = [dict(run_id=11,
            manifest=dict(identity, fingerprint=benchmark.fingerprint(identity)),
            environments=dict(evidence={environment_id: environment},
                              invocations={"preserved": dict(environment_fingerprint=environment_id)}))]
        seed = dict(active[0], status="ok", source_sha="a" * 40, elapsed_seconds=.1, ts_db=-29,
                    invocation_id="preserved", measurement_origin_run_id=11)
        sidecar["reused_records"] = {seed["case_id"]: seed}
        settings = dict(configuration=config, profile="pilot", software=software, repetitions=1,
                        source_ref="a" * 40, planned_cases=2, sources_fingerprint=benchmark.fingerprint(sources))
        metadata = dict(schema_version=1, repository="owner/repo", run_id=12, platform="ubuntu-latest",
                        settings=settings, sources=sources, reused_count=1,
                        retained_fingerprint=benchmark.fingerprint(sidecar))
        return {"refresh-campaign.json": metadata, "retained-results.json": sidecar,
                f"records/{seed['case_id']}.json": seed}

    @staticmethod
    def artifact_api(files, name="results-ubuntu-latest", run=None):
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
            for filename, value in files.items():
                archive.writestr(filename, json.dumps(value))
        api = Mock(repository="owner/repo")
        status = run or dict(status="completed", conclusion="failure", run_attempt=1)
        api.request.side_effect = lambda method, path: (dict(artifacts=[dict(id=7, name=name)])
            if "/artifacts?" in path else status)
        api.archive.return_value = stream.getvalue()
        return api

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

    def test_reference_subset_keeps_qualified_comparisons_from_the_saved_grid(self):
        import benchmark
        active, sidecar = self.retained_fixture()
        for index, row in enumerate(sidecar["rows"]):
            row.update(reference_id=str(index + 1) * 64, reference_ts_db=-31,
                       reference_uncertainty_db=.001, reference_qualification="fixture",
                       reference_source_sha="c" * 40, delta_ts_db=row["ts_db"] + 31)
        original = copy.deepcopy(sidecar)
        self.assertEqual(progress.retained_comparison_rows(sidecar, active, {}), sidecar["rows"])
        first = sidecar["rows"][0]
        reference = dict({key: first[key] for key in benchmark.PHYSICAL_FIELDS},
                         reference_id="d" * 64, source_sha="c" * 40, qualified=True,
                         qualification="fixture", software="acousticscattering", controls={},
                         ts_db=-32, uncertainty_db=.001)
        references = benchmark.references_from_value(dict(references=[reference]))
        joined = progress.retained_comparison_rows(sidecar, active, references)
        self.assertEqual(joined[0]["delta_ts_db"], first["ts_db"] + 32)
        self.assertEqual(joined[0]["reference_id"], "d" * 64)
        self.assertEqual(joined[1], sidecar["rows"][1])
        self.assertEqual(sidecar, original)

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

    def test_setup_keeps_selected_prol_measurements_visible_before_first_new_prediction(self):
        import benchmark
        files = self.setup_baseline_fixture("prol_spheroid")
        sidecar = files["retained-results.json"]
        seed = next(iter(sidecar["reused_records"].values()))
        seed.update(memory_delta_peak_bytes=4096, memory_baseline_rss_bytes=1048576,
                    memory_peak_rss_bytes=1052672, memory_method="sampled_process_tree_rss_above_baseline",
                    memory_status="ok")
        for name, value in files.items():
            benchmark.atomic_json(self.directory / name, value)
        before = {name: (self.directory / name).read_bytes() for name in files}
        metadata = files["refresh-campaign.json"]
        arguments = ["publish_progress.py", "--repository", "owner/repo", "--platform", "ubuntu-latest",
                     "--directory", str(self.directory), "--run-id", "124", "--state", "setup",
                     "--config", str(self.config_file(metadata["settings"]["configuration"])), "--profile", "pilot",
                     "--software", "prol_spheroid", "--repetitions", "1"]
        original_read = progress.read_json
        with patch.object(sys, "argv", arguments), patch.object(progress, "GitHub"), \
                patch.object(progress, "read_json", side_effect=lambda path: metadata["sources"]
                    if Path(path) == progress.ROOT / "sources.lock.json" else original_read(path)), \
                patch.object(progress, "publish", return_value=True) as publish:
            progress.main()
        payload = publish.call_args.args[1]
        self.assertEqual(payload["state"], "setup")
        self.assertEqual(payload["status"]["recorded"], 1)
        self.assertEqual(payload["status"]["outcomes"], dict(ok=1, pending=1))
        self.assertEqual(payload["active_software"], ["prol_spheroid"])
        self.assertEqual(payload["retained_count"], 2)
        active = [row for row in payload["rows"] if row["software"] == "prol_spheroid"]
        self.assertEqual(len(active), 1)
        for key in ("elapsed_seconds", "ts_db", "source_sha", "memory_delta_peak_bytes",
                    "memory_baseline_rss_bytes", "memory_peak_rss_bytes"):
            self.assertEqual(active[0][key], seed[key])
        retained = [row for row in payload["rows"] if row["software"] == "echosms"]
        for row, original in zip(retained, sidecar["rows"]):
            for key in ("elapsed_seconds", "ts_db", "source_sha", "retained_from_run_id"):
                self.assertEqual(row[key], original[key])
        self.assertEqual({name: (self.directory / name).read_bytes() for name in files}, before)
        self.assertFalse((self.directory / "results.json").exists())

    def test_setup_seed_requires_matching_saved_record_source_platform_and_prediction_inputs(self):
        import benchmark
        for change in ("missing", "elapsed", "source", "platform", "prediction", "provenance"):
            with self.subTest(change=change):
                files = self.setup_baseline_fixture("prol_spheroid")
                metadata = files["refresh-campaign.json"]
                sidecar = files["retained-results.json"]
                record_name = next(name for name in files if name.startswith("records/"))
                for name, value in files.items():
                    benchmark.atomic_json(self.directory / name, value)
                config = metadata["settings"]["configuration"]
                rows = progress.pending_setup_cases(self.config_file(config), "pilot", "prol_spheroid", 1)
                selected_platform = "ubuntu-latest"
                sources = copy.deepcopy(metadata["sources"])
                if change == "missing":
                    (self.directory / record_name).unlink()
                elif change == "elapsed":
                    record = copy.deepcopy(files[record_name])
                    record["elapsed_seconds"] = 999
                    benchmark.atomic_json(self.directory / record_name, record)
                elif change == "source":
                    sources["sources"]["prol_spheroid"]["sha"] = "b" * 40
                elif change == "platform":
                    selected_platform = "windows-latest"
                elif change == "prediction":
                    rows[0]["incidence_deg"] = 37
                else:
                    sidecar["provenance"][0]["manifest"]["workload"] = "different"
                    benchmark.atomic_json(self.directory / "retained-results.json", sidecar)
                original_read = progress.read_json
                with patch.object(progress, "read_json", side_effect=lambda path: sources
                        if Path(path) == progress.ROOT / "sources.lock.json" else original_read(path)), \
                        self.assertRaises(ValueError):
                    progress.setup_measurement_rows(self.directory, rows, selected_platform)

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

    def test_artifact_republication_accepts_large_reference_evidence_with_a_separate_bound(self):
        import benchmark
        active, sidecar = self.retained_fixture()
        reference = dict({key: sidecar["rows"][0][key] for key in benchmark.PHYSICAL_FIELDS},
            reference_id="d" * 64, source_sha="c" * 40, qualified=True, qualification="fixture",
            software="acousticscattering", controls={}, ts_db=-32, uncertainty_db=.01,
            diagnostics=dict(mesh="x" * 20000))
        reference_payload = dict(references=[reference])
        api = self.artifact_api({"results.json": active, "retained-results.json": sidecar,
                                 "references.json": reference_payload})
        length = len(json.dumps(reference_payload).encode())
        with patch.object(progress, "MAX_JSON_BYTES", 10000), \
                patch.object(progress, "MAX_REFERENCE_BYTES", length):
            payload = progress.artifact_payload(api, "ubuntu-latest", 12)
        self.assertEqual(payload["retained_count"], 2)
        self.assertEqual(payload["rows"][0]["delta_ts_db"], 2)
        self.assertEqual(payload["reference_curves"][0]["ts_db"], -32)
        self.assertNotIn("diagnostics", json.dumps(payload))
        with patch.object(progress, "MAX_REFERENCE_BYTES", length - 1):
            with self.assertRaisesRegex(ValueError, "oversized"):
                progress.artifact_payload(api, "ubuntu-latest", 12)

    def test_failed_setup_publishes_validated_seed_and_retained_measurements(self):
        files = self.setup_baseline_fixture()
        for name in ("results-ubuntu-latest", "retained-ubuntu-latest"):
            with self.subTest(artifact=name):
                payload = progress.artifact_payload(self.artifact_api(files, name), "ubuntu-latest", 12)
                self.assertEqual(payload["state"], "completed")
                self.assertEqual(payload["run_conclusion"], "failure")
                self.assertEqual(payload["status"], dict(state="setup_failed", planned=2, recorded=1,
                                                       outcomes=dict(ok=1, pending=1)))
                self.assertEqual(payload["retained_count"], 2)
                active = [row for row in payload["rows"] if row.get("measurement_role") != "retained"]
                self.assertEqual(len(active), 1)
                self.assertEqual(active[0]["elapsed_seconds"], .1)
                self.assertEqual(active[0]["ts_db"], -29)
        for alteration in ("missing_seed", "changed_seed", "wrong_run"):
            changed = copy.deepcopy(files)
            seed = next(name for name in changed if name.startswith("records/"))
            if alteration == "missing_seed":
                changed.pop(seed)
            elif alteration == "changed_seed":
                changed[seed]["elapsed_seconds"] = 999
            else:
                changed["refresh-campaign.json"]["run_id"] = 99
            with self.subTest(alteration=alteration), self.assertRaises(ValueError):
                progress.artifact_payload(self.artifact_api(changed), "ubuntu-latest", 12)

    def test_empty_or_unvalidated_setup_artifact_does_not_invent_results(self):
        files = self.setup_baseline_fixture()
        for name in ("refresh-campaign.json", "retained-results.json"):
            changed = dict(files)
            changed.pop(name)
            with self.subTest(missing=name), self.assertRaises(ValueError):
                progress.artifact_payload(self.artifact_api(changed), "ubuntu-latest", 12)

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
        api = GitAPI({"ubuntu-latest.json": self.payload(11)})
        api.conflict = {"ubuntu-latest.json": newer}
        self.assertFalse(progress.publish(api, proposed))
        self.assertEqual(api.current("ubuntu-latest.json"), newer)
        self.assertEqual(sum(call.args[0] == "PATCH" for call in api.request.call_args_list), 1)

    def test_size_and_latest_snapshots_publish_in_one_atomic_commit(self):
        proposed = dict(self.payload(12), semimajor_m=.01)
        previous = dict(self.payload(11), semimajor_m=.07)
        api = GitAPI({"ubuntu-latest.json": previous, "dataset.json": {"preserved": True}})
        self.assertTrue(progress.publish(api, proposed))
        self.assertEqual(api.current("ubuntu-latest-a-0.01.json"), proposed)
        self.assertEqual(api.current("ubuntu-latest.json"), proposed)
        self.assertEqual(api.current("dataset.json"), {"preserved": True})
        self.assertEqual(sum(call.args[1] == "/git/blobs" for call in api.request.call_args_list), 1)
        self.assertEqual(sum(call.args[0] == "PATCH" for call in api.request.call_args_list), 1)
        tree = api.trees[api.commits[api.head]["tree"]]
        self.assertEqual(tree["ubuntu-latest.json"], tree["ubuntu-latest-a-0.01.json"])

    def test_older_size_can_update_without_overwriting_latest_other_size(self):
        proposed = dict(self.payload(12), semimajor_m=.07)
        newer = dict(self.payload(13), semimajor_m=.01)
        api = GitAPI({"ubuntu-latest-a-0.07.json": dict(self.payload(11), semimajor_m=.07), "ubuntu-latest.json": newer})
        self.assertTrue(progress.publish(api, proposed))
        self.assertEqual(api.current("ubuntu-latest-a-0.07.json"), proposed)
        self.assertEqual(api.current("ubuntu-latest.json"), newer)

    def test_size_conflict_preserves_newer_size_and_latest_snapshots(self):
        proposed = dict(self.payload(12), semimajor_m=.01)
        old = dict(self.payload(11), semimajor_m=.01)
        newer = dict(self.payload(13), semimajor_m=.01)
        names = ("ubuntu-latest-a-0.01.json", "ubuntu-latest.json")
        api = GitAPI({name: old for name in names})
        api.conflict = {name: newer for name in names}
        self.assertFalse(progress.publish(api, proposed))
        for name in names:
            self.assertEqual(api.current(name), newer)

    def test_failed_reference_update_preserves_both_snapshot_aliases(self):
        previous = dict(self.payload(11), semimajor_m=.1)
        names = ("ubuntu-latest-a-0.1.json", "ubuntu-latest.json")
        api = GitAPI({name: previous for name in names})
        initial_head = api.head
        api.ref_error = progress.APIError(500, "Failed")
        with self.assertRaises(progress.APIError):
            progress.publish(api, dict(self.payload(), semimajor_m=.1))
        self.assertEqual(api.head, initial_head)
        for name in names:
            self.assertEqual(api.current(name), previous)

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

    def test_large_snapshot_compression_is_lossless_and_deterministic(self):
        payload = dict(self.payload(), semimajor_m=.01, padding="repeated scientific data " * 8000,
                       precision_examples=[0, None, -0.0, 1e-17, -123.45678901234567])
        encoded = progress.encode_snapshot(payload)
        self.assertEqual(encoded["schema_version"], 2)
        self.assertEqual(encoded["encoding"], "gzip-base64")
        self.assertEqual(encoded["status"], payload["status"])
        self.assertEqual(encoded, progress.encode_snapshot(payload))
        self.assertEqual(progress.decode_snapshot(encoded), payload)
        self.assertLess(len(json.dumps(encoded)), encoded["decoded_bytes"] // 10)
        self.assertEqual(json.loads(gzip.decompress(base64.b64decode(encoded["data"]))), payload)

    def test_small_snapshot_remains_compatible_with_existing_reader(self):
        payload = self.payload()
        self.assertEqual(progress.encode_snapshot(payload), payload)
        self.assertEqual(progress.encode_snapshot(payload)["schema_version"], 1)

    def test_compressed_snapshot_rejects_size_and_guard_tampering(self):
        payload = dict(self.payload(), padding="x" * 70000)
        value = progress.encode_snapshot(payload)
        with self.assertRaisesRegex(ValueError, "size"):
            progress.decode_snapshot(dict(value, decoded_bytes=value["decoded_bytes"] - 1))
        with self.assertRaisesRegex(ValueError, "metadata"):
            progress.decode_snapshot(dict(value, run_id=value["run_id"] + 1))
        with self.assertRaisesRegex(ValueError, "Invalid compressed"):
            progress.decode_snapshot(dict(value, decoded_bytes=progress.MAX_SNAPSHOT_BYTES + 1))
        with patch.object(progress, "MAX_SNAPSHOT_BYTES", 100):
            with self.assertRaisesRegex(ValueError, "size"):
                progress.encode_snapshot(payload)

    def test_existing_snapshot_transparently_decodes_envelope(self):
        payload = dict(self.payload(), padding="x" * 70000)
        encoded = progress.encode_snapshot(payload)
        api = Mock()
        api.request.return_value = dict(sha="compressed-sha", encoding="base64",
            content=base64.b64encode(json.dumps(encoded).encode()).decode())
        result, sha = progress.existing_snapshot(api, "ubuntu-latest")
        self.assertEqual(result, payload)
        self.assertEqual(sha, "compressed-sha")

    def test_cached_blob_metadata_avoids_downloading_previous_large_snapshots(self):
        initial = dict(self.payload(11), semimajor_m=.01, padding="x" * 70000)
        names = ("ubuntu-latest-a-0.01.json", "ubuntu-latest.json")
        api = GitAPI({name: progress.encode_snapshot(initial) for name in names})
        proposed = dict(initial, run_id=12)
        self.assertTrue(progress.publish(api, proposed))
        downloads = sum(call.args[0] == "GET" and call.args[1].startswith("/git/blobs/") for call in api.request.call_args_list)
        self.assertEqual(downloads, 1)
        self.assertTrue(progress.publish(api, dict(proposed, run_id=13)))
        self.assertEqual(sum(call.args[0] == "GET" and call.args[1].startswith("/git/blobs/") for call in api.request.call_args_list), downloads)
        for name in names:
            self.assertEqual(progress.decode_snapshot(api.current(name))["run_id"], 13)

    def test_conflict_reuses_blob_and_preserves_another_writers_updates(self):
        proposed = dict(self.payload(12), semimajor_m=.01)
        api = GitAPI({"ubuntu-latest.json": self.payload(11), "dataset.json": {"generation": 1}})
        api.conflict = {"dataset.json": {"generation": 2}, "queue.json": {"keep": True}}
        self.assertTrue(progress.publish(api, proposed))
        self.assertEqual(api.current("dataset.json"), {"generation": 2})
        self.assertEqual(api.current("queue.json"), {"keep": True})
        self.assertEqual(api.current("ubuntu-latest.json"), proposed)
        self.assertEqual(api.current("ubuntu-latest-a-0.01.json"), proposed)
        self.assertEqual(sum(call.args[0] == "POST" and call.args[1] == "/git/blobs" for call in api.request.call_args_list), 1)

    def test_safe_errors_include_status_and_endpoint_but_never_query_or_credentials(self):
        error = progress.APIError(409, "secret response", method="PATCH", path="/git/refs/heads/benchmark-data?token=secret")
        self.assertEqual(progress.safe_error_summary(error), "APIError HTTP 409 during PATCH /git/refs/heads/benchmark-data")
        api = progress.GitHub("owner/repo", "secret")
        api.opener = Mock()
        api.opener.open.side_effect = URLError("https://secret@example.com/?token=secret")
        with self.assertRaises(URLError) as raised:
            api.request("GET", "/git/trees/test?private=secret")
        self.assertEqual(progress.safe_error_summary(raised.exception), "URLError during GET /git/trees/test")

    def test_bootstrap_commit_contains_only_platform_data_and_no_parent(self):
        api = GitAPI(empty=True)
        self.assertTrue(progress.publish(api, self.payload()))
        commit = api.commits[api.head]
        self.assertEqual(commit["parents"], [])
        self.assertEqual(set(api.trees[commit["tree"]]), {"ubuntu-latest.json"})


if __name__ == "__main__":
    unittest.main()
