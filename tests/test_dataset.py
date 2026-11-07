"""Identity, history, and atomicity of the consolidated benchmark dataset."""

import copy
import csv
import gzip
import json
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import benchmark
import dataset


def case(software="acousticts", boundary="gas"):
    physical = dict(boundary=boundary, frequency_hz=1000, incidence_deg=37.0,
        semimajor_m=.01, semiminor_m=.001, medium_density_kg_m3=1026.8,
        medium_sound_speed_m_s=1477.3, target_density_kg_m3=1.24,
        target_sound_speed_m_s=345.0)
    result = dict(physical, software=software, repetition=1, aspect_ratio=10.,
        k0a=.04, h0=.03, hs=.1, controls={}, warmup=True,
        physical_id=benchmark.physical_identity(physical))
    return dict(result, case_id=benchmark.fingerprint(result))


def record(prediction, source="a" * 40, invocation="first", finished="2026-10-24T01:00:00+00:00"):
    return dict(case_id=prediction["case_id"], source_sha=source, invocation_id=invocation,
        finished_at=finished, status="ok", ts_db=-40., elapsed_seconds=.2,
        memory_delta_peak_bytes=1024, diagnostics={"original": True})


def manifest(predictions, source="a" * 40):
    value = dict(configuration={}, sources={"sources": {c["software"]: {"sha": source} for c in predictions}},
        profile="test", cases=[c["case_id"] for c in predictions], adapter_hashes={"historical_worker": "b" * 64},
        platform="Linux", architecture="x86_64", environment_fingerprint="c" * 64, workload="historical")
    return dict(value, fingerprint=benchmark.fingerprint(value), reference_requests=[{"unused": [1, 2, 3]}])


def files(prediction=None, observation=None, source="a" * 40, include_records=True):
    prediction = prediction or case()
    observation = observation or record(prediction, source)
    result = {"manifest.json": manifest([prediction], source),
              "results.json": [dict(prediction, **{k: v for k, v in observation.items() if k not in prediction})]}
    if include_records:
        result[f"records/{prediction['case_id']}.json"] = observation
    return result


class DatasetTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.connection = dataset.connect_database(self.root / "data.sqlite")
        self.addCleanup(self.connection.close)

    def archive(self, contents, number):
        path = self.root / f"{number}.zip"
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
            for name, value in contents.items():
                archive.writestr(name, json.dumps(value))
        return path

    def load(self, contents, number=1, name="results-ubuntu-latest", run=None):
        return dataset.import_artifact(self.connection, self.archive(contents, number),
            dict(id=number, name=name, workflow_run={"id": run or number}, created_at=f"2026-10-26T00:00:{number:02}+00:00"),
            "owner/repository")

    def test_repeated_checkpoint_and_retained_baseline_deduplicate(self):
        prediction = case()
        observation = record(prediction)
        original = files(prediction, observation)
        self.assertEqual(self.load(original)["observations"], 1)
        self.assertTrue(self.load(original)["skipped"])
        self.assertEqual(self.load(original, 2)["observations"], 0)
        baseline = {"refresh-campaign.json": dict(repository="owner/repository", platform="ubuntu-latest"),
            "retained-results.json": dict(schema_version=1, rows=[], provenance=[],
                reused_records={prediction["case_id"]: dict(prediction, **{k: v for k, v in observation.items() if k not in prediction},
                    measurement_origin_run_id=1)})}
        self.assertEqual(self.load(baseline, 3, "retained-ubuntu-latest")["observations"], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM observation_artifacts").fetchone()[0], 3)

    def test_software_versions_and_real_reruns_remain_distinct(self):
        prediction = case()
        self.load(files(prediction, record(prediction)))
        self.load(files(prediction, record(prediction, "b" * 40, "second", "2026-10-25T01:00:00+00:00"), "b" * 40), 2)
        self.load(files(prediction, record(prediction, "b" * 40, "third", "2026-10-26T01:00:00+00:00"), "b" * 40), 3)
        self.load(files(prediction, record(prediction)), 4)
        self.assertEqual(dataset.database_summary(self.connection)["measurements"], 3)
        self.assertEqual(self.connection.execute("SELECT invocation_id FROM latest_measurements").fetchone()[0], "third")
        output = self.root / "measurements.csv.gz"
        self.assertEqual(dataset.export_csv(self.connection, output), 3)
        with gzip.open(output, "rt", encoding="utf-8") as stream:
            self.assertEqual({r["source_sha"] for r in csv.DictReader(stream)}, {"a" * 40, "b" * 40})

    def test_conflicting_same_invocation_rolls_back_entire_artifact(self):
        prediction = case()
        self.load(files(prediction))
        conflict = files(prediction, dict(record(prediction), elapsed_seconds=99.))
        with self.assertRaisesRegex(ValueError, "Conflicting measurements"):
            self.load(conflict, 2)
        self.assertEqual(dataset.database_summary(self.connection)["artifacts"], 1)
        self.assertEqual(self.connection.execute("SELECT elapsed_seconds FROM observations").fetchone()[0], .2)

    def test_only_structurally_inapplicable_cases_are_excluded(self):
        prediction = case("prol_spheroid", "rigid")
        self.load(files(prediction, dict(record(prediction), status="unsupported", ts_db=None, elapsed_seconds=None)))
        other = case("acousticts", "gas")
        self.load(files(other, dict(record(other), status="unsupported", ts_db=None, elapsed_seconds=None)), 2)
        self.assertEqual(dataset.database_summary(self.connection)["observations"], 2)
        self.assertEqual(dataset.database_summary(self.connection)["measurements"], 1)
        self.assertEqual(self.connection.execute("SELECT software FROM measurements").fetchone()[0], "acousticts")

    def test_original_environment_survives_later_copy(self):
        prediction = case()
        content = files(prediction)
        content["environments/first.json"] = dict(platform="Linux", github_run_id="123",
            github_run_attempt="2", started_at="2026-10-24T00:59:00+00:00", runtime={"historical": True})
        self.load(content, run=999)
        self.assertEqual(tuple(self.connection.execute("SELECT origin_run_id, origin_run_attempt FROM observations").fetchone()), (123, 2))

    def test_retained_proof_uses_old_adapter_versions_and_streams_excluded_plans(self):
        prediction = case()
        evidence = dict(platform="Linux", github_run_id="123", github_run_attempt="1", runtime={"historical": True})
        key = benchmark.fingerprint(evidence)
        proof = dict(manifest=manifest([prediction]), run_id=123,
            environments=dict(invocations={"first": dict(environment_fingerprint=key, started_at="2026-10-24T00:00:00+00:00")}, evidence={key: evidence}))
        content = {"retained-results.json": dict(schema_version=1,
            rows=[dict(prediction, **{k: v for k, v in record(prediction).items() if k not in prediction},
                       measurement_role="retained", retained_from_run_id=456)], provenance=[proof])}
        self.load(content, name="retained-ubuntu-latest")
        self.assertEqual(self.connection.execute("SELECT origin_run_id FROM observations").fetchone()[0], 123)

    def test_compared_artifact_enriches_without_duplicating_timings(self):
        prediction = case()
        self.load(files(prediction))
        compared = files(prediction, include_records=False)
        compared["results.json"][0].update(reference_id="b" * 64, reference_fingerprint="c" * 64,
            reference_source_sha="d" * 40, reference_ts_db=-41., reference_uncertainty_db=.1,
            reference_qualification={"spatial_pass": True}, delta_ts_db=1.)
        self.assertEqual(self.load(compared, 2, "compared-results-ubuntu-latest")["observations"], 0)
        self.assertEqual(self.connection.execute("SELECT delta_ts_db FROM comparisons").fetchone()[0], 1.)
        output = self.root / "comparisons.csv.gz"
        self.assertEqual(dataset.export_comparisons_csv(self.connection, output), 1)
        with gzip.open(output, "rt", encoding="utf-8") as stream:
            exported = next(csv.DictReader(stream))
        self.assertEqual(exported["delta_ts_db"], "1.0")
        self.assertEqual(exported["source_sha"], "a" * 40)
        self.assertEqual(exported["reference_source_sha"], "d" * 40)
        compared["results.json"][0]["delta_ts_db"] = 2.
        with self.assertRaisesRegex(ValueError, "TS difference"):
            self.load(compared, 3, "compared-results-ubuntu-latest")

    def test_comparison_import_before_original_measurement_recovers_origin(self):
        prediction = case()
        copied = files(prediction, include_records=False)
        copied["results.json"][0].update(reference_id="b" * 64, reference_ts_db=-41., delta_ts_db=1.)
        self.load(copied, 1, "compared-results-ubuntu-latest", run=456)
        self.assertIsNone(self.connection.execute("SELECT origin_run_id FROM observations").fetchone()[0])
        original = files(prediction)
        original["environments/first.json"] = dict(platform="Linux", github_run_id="123", github_run_attempt="2")
        self.assertEqual(self.load(original, 2, run=123)["observations"], 0)
        self.assertEqual(tuple(self.connection.execute("SELECT origin_run_id, origin_run_attempt FROM observations").fetchone()), (123, 2))
        self.assertEqual(dataset.database_summary(self.connection)["comparisons"], 1)

    def test_case_and_atomic_record_tampering_are_rejected(self):
        broken = files()
        broken["results.json"][0]["frequency_hz"] = 2000
        with self.assertRaisesRegex(ValueError, "Case inputs"):
            self.load(broken)
        broken = files()
        broken["results.json"][0]["elapsed_seconds"] = 99.
        with self.assertRaisesRegex(ValueError, "atomic measurement"):
            self.load(broken, 2)
        self.assertEqual(dataset.database_summary(self.connection)["artifacts"], 0)

    def test_setup_only_is_recognized_and_interrupted_atomic_records_import(self):
        self.assertEqual(self.load({"benchmark-source.json": {"repository": "owner/repository"}})["observations"], 0)
        interrupted = files()
        interrupted["results.json"] = [dict(case(), status="pending")]
        self.assertEqual(self.load(interrupted, 2)["observations"], 1)

    def test_legacy_missing_invocation_deduplicates_by_original_record(self):
        prediction = case()
        observation = record(prediction)
        observation.pop("invocation_id")
        self.load(files(prediction, observation))
        self.load(files(prediction, observation), 2)
        self.assertEqual(dataset.database_summary(self.connection)["observations"], 1)

    def test_reference_records_are_deduplicated_by_full_evidence_identity(self):
        prediction = case()
        physical = {key: prediction[key] for key in benchmark.PHYSICAL_FIELDS}
        signature = dict(physical=physical, source_sha="e" * 40, controls={}, method="fem_bem")
        reference = dict(physical, physical_id=prediction["physical_id"], signature=signature,
            reference_id=benchmark.fingerprint(signature), source_sha="e" * 40,
            software="acousticscattering", method="fem_bem", controls={}, qualified=True,
            status="ok", ts_db=-41., uncertainty_db=.1, qualification={"spatial_pass": True},
            evidence={"large_nested_data": [1, 2, 3]}, finished_at="2026-10-24T01:00:00+00:00")
        contents = {"references.json": {"references": [reference]}}
        self.assertEqual(self.load(contents, name="references-0-1")["references"], 1)
        self.assertEqual(tuple(self.connection.execute("SELECT boundary, semimajor_m, aspect_ratio, incidence_deg, frequency_hz FROM reference_results").fetchone()),
                         ("gas", .01, 10., 37., 1000.))
        self.assertEqual(self.load(contents, 2, "references-1-1")["references"], 0)
        changed = copy.deepcopy(contents)
        changed["references.json"]["references"][0]["signature"]["source_sha"] = "f" * 40
        with self.assertRaisesRegex(ValueError, "Reference inputs"):
            self.load(changed, 3, "references-1-2")

    def test_json_parser_omits_scalar_and_nested_fields_without_losing_neighbors(self):
        path = self.archive({"test.json": {"before": 1, "omit": [{"x": [2, 3]}], "after": 4, "small": None, "tail": 5}}, 1)
        with zipfile.ZipFile(path) as archive:
            self.assertEqual(dataset._object(archive, "test.json", omit=("omit", "small")), {"before": 1, "after": 4, "tail": 5})



    def spectrum_exports(self):
        first = case()
        second = {key: value for key, value in dict(first, repetition=2).items() if key != "case_id"}
        second["case_id"] = benchmark.fingerprint(second)
        content = {}
        for task_id, prediction, invocation in (("a" * 24, first, "first"), ("b" * 24, second, "second")):
            exported = files(prediction, record(prediction, invocation=invocation))
            exported[f"environments/{invocation}.json"] = dict(
                platform="Linux", github_run_id="123", github_run_attempt="2", runtime={"historical": True})
            content.update({f"spectrum/{task_id}/{name}": value for name, value in exported.items()})
        content["benchmark-env/runtime.json"] = {"runtime": "unrelated setup metadata"}
        return content

    def test_nested_frequency_worker_exports_import_without_legacy_bundle_descriptor(self):
        content = self.spectrum_exports()
        name = "spectrum-" + "c" * 24 + "-1"
        imported = self.load(content, name=name, run=456)
        self.assertEqual(imported["observations"], 2)
        self.assertEqual(imported["cases"], 2)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM manifests").fetchone()[0], 2)
        self.assertEqual({tuple(row) for row in self.connection.execute(
            "SELECT origin_run_id, origin_run_attempt FROM observations")}, {(123, 2)})
        self.assertEqual(self.load(content, 2, name=name, run=789)["observations"], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM observation_artifacts").fetchone()[0], 4)

    def test_nested_spectrum_import_rolls_back_valid_earlier_exports_after_later_corruption(self):
        for change in ("source", "atomic_record", "manifest"):
            with self.subTest(change=change):
                content = self.spectrum_exports()
                prefix = "spectrum/" + "b" * 24 + "/"
                if change == "source":
                    content[prefix + "results.json"][0]["source_sha"] = "f" * 40
                    raw = next(name for name in content if name.startswith(prefix + "records/"))
                    content[raw]["source_sha"] = "f" * 40
                elif change == "atomic_record":
                    content[prefix + "results.json"][0]["elapsed_seconds"] = 99
                else:
                    content[prefix + "manifest.json"]["fingerprint"] = "f" * 64
                with self.assertRaises(ValueError):
                    self.load(content, name="spectrum-" + "c" * 24 + "-1")
                self.assertEqual(dataset.database_summary(self.connection)["artifacts"], 0)
                self.assertEqual(dataset.database_summary(self.connection)["observations"], 0)
                self.assertEqual(self.connection.execute("SELECT count(*) FROM manifests").fetchone()[0], 0)

    def test_nested_spectrum_groups_reject_incomplete_mixed_or_undeclared_exports(self):
        for change in ("missing_results", "mixed_roots", "invalid_task", "undeclared_results"):
            with self.subTest(change=change):
                content = self.spectrum_exports()
                prefix = "spectrum/" + "b" * 24 + "/"
                if change == "missing_results":
                    del content[prefix + "results.json"]
                elif change in ("mixed_roots", "invalid_task"):
                    replacement = ("other/" + "b" * 24 + "/"
                                   if change == "mixed_roots" else "spectrum/not-a-task/")
                    content = {name.replace(prefix, replacement): value for name, value in content.items()}
                else:
                    content["spectrum/" + "d" * 24 + "/results.json"] = []
                with self.assertRaises(ValueError):
                    self.load(content, name="spectrum-" + "c" * 24 + "-1")
                self.assertEqual(dataset.database_summary(self.connection)["artifacts"], 0)

    def test_nested_inference_preserves_declared_bundle_validation_and_legacy_ambiguity(self):
        content = self.spectrum_exports()
        with self.assertRaisesRegex(ValueError, "Ambiguous artifact metadata"):
            self.load(content, name="results-ubuntu-latest")
        content["spectrum/spectrum-bundle.json"] = dict(
            schema_version=1, campaign_id="123", worker_id="c" * 24, task_ids=["a" * 24, "b" * 24])
        self.assertEqual(self.load(content, name="spectrum-" + "c" * 24 + "-1")["observations"], 2)
        broken = copy.deepcopy(content)
        broken["spectrum/spectrum-bundle.json"]["task_ids"] = ["a" * 24]
        with self.assertRaisesRegex(ValueError, "undeclared spectrum"):
            self.load(broken, 2, name="spectrum-" + "c" * 24 + "-1")
        self.assertEqual(dataset.database_summary(self.connection)["artifacts"], 1)
        self.assertEqual(dataset.database_summary(self.connection)["observations"], 2)

    def test_inferred_timeout_provenance_survives_dataset_and_progress_without_fake_measurements(self):
        import publish_progress as progress
        import zlib

        prediction = case()
        basis = dict(rule="ten_consecutive_prediction_timeouts",
                     trigger_case_ids=[f"{index:064x}" for index in range(10)])
        observation = dict(case_id=prediction["case_id"], source_sha="a" * 40,
                           invocation_id="stopped-tail", finished_at="2026-11-07T00:00:00+00:00",
                           status="timeout", prediction_attempted=False, timeout_basis=basis)
        self.load(files(prediction, observation))
        saved = self.connection.execute(
            "SELECT elapsed_seconds, wall_seconds, ts_db, record_zlib FROM observations").fetchone()
        self.assertEqual(tuple(saved[:3]), (None, None, None))
        record = json.loads(zlib.decompress(saved["record_zlib"]))
        self.assertIs(record["prediction_attempted"], False)
        self.assertEqual(record["timeout_basis"], basis)
        row = files(prediction, observation)["results.json"][0]
        original = copy.deepcopy(row)
        payload = progress.compact_payload("owner/repository", "ubuntu-latest", 12, 1, [row])
        exported = payload["rows"][0]
        self.assertIs(exported["prediction_attempted"], False)
        self.assertEqual(exported["timeout_basis"], basis)
        self.assertIsNone(exported["elapsed_seconds"])
        self.assertIsNone(exported["ts_db"])
        self.assertIsNone(exported["delta_ts_db"])
        self.assertEqual(payload["coverage"][0]["success"], 0)
        self.assertEqual(payload["coverage"][0]["other_recorded"], 1)
        self.assertEqual(payload["coverage"][0]["elapsed_seconds_sum"], 0)
        exported["timeout_basis"]["trigger_case_ids"].reverse()
        self.assertEqual(row, original)
        historical = progress.compact_payload("owner/repository", "ubuntu-latest", 12, 1,
                                             [dict(prediction, status="timeout")])["rows"][0]
        self.assertNotIn("prediction_attempted", historical)
        self.assertNotIn("timeout_basis", historical)

    def test_progress_rejects_invalid_inferred_timeout_proof_and_prediction_values(self):
        import publish_progress as progress

        row = dict(case(), status="timeout", prediction_attempted=False,
                   timeout_basis=dict(rule="ten_consecutive_prediction_timeouts",
                                      trigger_case_ids=[f"{index:064x}" for index in range(10)]))
        invalid = []
        for attempted in (0, "false", True, None):
            invalid.append(dict(row, prediction_attempted=attempted))
        invalid += [dict(row, status="ok"), dict(row, timeout_basis=None)]
        for change in (
                dict(rule="unsupported_rule", trigger_case_ids=row["timeout_basis"]["trigger_case_ids"]),
                dict(row["timeout_basis"], extra="unverified"),
                dict(row["timeout_basis"], trigger_case_ids=["a" * 64] * 10),
                dict(row["timeout_basis"], trigger_case_ids=["invalid"] * 10),
                dict(row["timeout_basis"], trigger_case_ids=row["timeout_basis"]["trigger_case_ids"][:9]),
                dict(row["timeout_basis"], trigger_case_ids=None)):
            invalid.append(dict(row, timeout_basis=change))
        for field in ("elapsed_seconds", "wall_seconds", "solve_lower_bound_seconds", "ts_db",
                      "delta_ts_db", "amplitude_real_m", "amplitude_imag_m"):
            invalid.append(dict(row, **{field: 0}))
        for index, corrupt in enumerate(invalid):
            with self.subTest(case=index), self.assertRaises(ValueError):
                progress.compact_payload("owner/repository", "ubuntu-latest", 12, 1, [corrupt])


if __name__ == "__main__":
    unittest.main()

