"""Editable campaign inputs and reconstruction of immutable timing artifacts."""

import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import benchmark
import campaign


def configuration():
    return dict(schema_version=1, geometry=dict(semimajor_m=.07, aspect_ratios=[1.001, 10]),
        incidences_deg=[0, 37, 90], boundaries=["rigid", "pressure_release", "liquid", "gas"],
        software=["acousticts", "prol_spheroid", "echosms", "liquid_spheroid"], repetitions=1,
        warmup=True, case_timeout_seconds=600,
        frequency=dict(start_hz=1000, stop_hz=200000, step_hz=2000, include_stop=True),
        medium=dict(density_kg_m3=1026.8, sound_speed_m_s=1477.3),
        materials=dict(liquid=dict(density_kg_m3=1028.9, sound_speed_m_s=1480.3),
                       gas=dict(density_kg_m3=1.24, sound_speed_m_s=345.0)),
        controls={name: {} for name in benchmark.CAPABILITIES},
        profiles=dict(pilot=dict(aspect_ratios=[10], incidences_deg=[90], boundaries=["gas"]), full={}))


def checkpoint(platform="ubuntu-latest", programs=None, repeats=3, count=2, source_ref=None, alter=None):
    config = configuration()
    config["frequency"] = dict(start_hz=1000, stop_hz=3000, step_hz=1000)
    programs = programs or ["echosms", "acousticts"]
    cases = benchmark.build_cases(config, "pilot", programs, repeats)
    lock = {"sources": {name: {"sha": "a" * 40} for name in benchmark.CAPABILITIES}}
    identity = dict(configuration=config, sources=lock, profile="pilot", cases=[case["case_id"] for case in cases],
        adapter_hashes={"worker.py": "b" * 64},
        platform={"ubuntu-latest": "Linux", "windows-latest": "Windows", "macos-latest": "Darwin"}[platform],
        architecture="x86_64", environment_fingerprint="c" * 64,
        workload="fresh_process_warmed_distinct_problem_single_prediction")
    records = {case["case_id"]: dict(case_id=case["case_id"], status="ok", elapsed_seconds=.2,
                    ts_db=-40, source_sha="a" * 40, invocation_id="preserved") for case in cases[:count]}
    files = {"manifest.json": dict(identity, fingerprint=benchmark.fingerprint(identity)),
             "results.json": benchmark.summarize(cases, records, {}),
             "status.json": dict(state="budget_paused", recorded=count, planned=len(cases)),
             **{f"records/{key}.json": record for key, record in records.items()}}
    if source_ref:
        files["benchmark-source.json"] = dict(schema_version=1, repository="owner/repo", source_ref=source_ref)
    if alter:
        alter(files)
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, value in files.items():
            archive.writestr(name, json.dumps(value))
    return stream.getvalue(), config


class CampaignTests(unittest.TestCase):
    def api(self, entries, payloads):
        api = Mock()
        run = dict(repository=dict(full_name="owner/repo"), head_repository=dict(full_name="owner/repo"),
                   event="workflow_dispatch", status="completed", path=".github/workflows/benchmark.yml",
                   head_sha="d" * 40)
        api.request.side_effect = lambda method, path: ({"artifacts": entries} if "/artifacts?" in path else run)
        api.archive.side_effect = lambda identity: payloads[identity]
        return api, run

    def resolve(self, inputs=None, api=None):
        return campaign.resolve(inputs or {}, configuration(), "owner/repo", "e" * 40, api)

    def test_new_defaults_are_two_software_and_dense_300khz(self):
        result = self.resolve()
        self.assertEqual(result["mode"], "new")
        self.assertEqual(result["software"], "acousticts,prol_spheroid")
        self.assertEqual(result["planned_cases"], 3624)
        config = result["configuration"]
        self.assertEqual(config["frequency"], dict(start_hz=1000, stop_hz=300000, step_hz=2000, include_stop=True))
        cases = benchmark.build_cases(config, result["profile"])
        self.assertEqual({case["aspect_ratio"] for case in cases}, {10})
        self.assertEqual({case["incidence_deg"] for case in cases}, {0, 37, 90})
        self.assertEqual({case["boundary"] for case in cases}, set(benchmark.BOUNDARIES))
        self.assertEqual(config["case_order"], "frequency_round_robin")
        self.assertEqual(cases[-1]["frequency_hz"], 300000)
        self.assertEqual(result["platforms"], ["ubuntu-latest"])

    def test_custom_dimensions_angles_materials_and_exact_decimal_hz(self):
        result = self.resolve(dict(mode="prepare", platform="all", software="Prol_Spheroid, acousticTS",
            frequency_start_khz="0.125", frequency_end_khz="0.129", frequency_interval_khz="0.002",
            semimajor_m="0.001", aspect_ratios="1.001, 10", incidences_deg="0,37,90",
            boundaries="liquid,gas", repetitions="3"))
        config = result["configuration"]
        self.assertEqual(result["mode"], "prepare")
        self.assertEqual(result["platforms"], list(campaign.PLATFORMS))
        self.assertEqual(result["software"], "prol_spheroid,acousticts")
        self.assertEqual(benchmark.frequencies(config), [125, 127, 129])
        self.assertEqual(config["geometry"], dict(semimajor_m=.001, aspect_ratios=[1.001, 10]))
        self.assertEqual(config["medium"], configuration()["medium"])
        self.assertEqual(config["materials"], configuration()["materials"])
        self.assertEqual(result["planned_cases"], 2 * 3 * 2 * 3 * 2 * 3)

    def test_invalid_inputs_are_rejected_before_building_large_grids(self):
        for inputs in (
            {"frequency_start_khz": 0}, {"frequency_end_khz": -1}, {"frequency_interval_khz": 0},
            {"frequency_interval_khz": ".0001"}, {"frequency_start_khz": 301},
            {"frequency_interval_khz": ".0009999999999999999999999999999999999"},
            {"frequency_end_khz": "Infinity"}, {"semimajor_m": 0}, {"semimajor_m": "NaN"},
            {"aspect_ratios": "1"}, {"aspect_ratios": "10,10.0"}, {"aspect_ratios": "inf"},
            {"incidences_deg": "91"}, {"incidences_deg": "37,37"}, {"boundaries": "gas,,liquid"},
            {"boundaries": "unknown"}, {"software": "acousticscattering"}, {"software": "acousticts,acousticts"},
            {"software": ""}, {"repetitions": 2}, {"repetitions": True}, {"platform": "unknown"},
            {"mode": "restart"}, {"frequency_end_khz": "1000000000", "frequency_interval_khz": ".001"}):
            with self.subTest(inputs=inputs), self.assertRaises(ValueError):
                self.resolve(inputs)

    def test_resume_recovers_actual_cli_selection_and_ignores_new_run_fields(self):
        content, original = checkpoint()
        api, _ = self.api([dict(id=1, name="results-ubuntu-latest")], {1: content})
        result = self.resolve(dict(mode="resume", resume_run_id="123", platform="invalid", software="invalid",
            repetitions=0, frequency_end_khz=-1, aspect_ratios="1", semimajor_m="NaN"), api)
        self.assertEqual(result["software"], "echosms,acousticts")
        self.assertEqual(result["repetitions"], 3)
        self.assertEqual(result["configuration"], original)
        self.assertEqual(result["profile"], "pilot")
        self.assertEqual(result["source_ref"], "d" * 40)
        self.assertEqual(result["planned_cases"], 18)
        self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 2})
        self.assertEqual(result["resume_artifacts"], {"ubuntu-latest": "results-ubuntu-latest"})

    def test_imported_resolver_can_authenticate_resume_from_workflow_environment(self):
        content, _ = checkpoint()
        api, _ = self.api([dict(id=1, name="results-ubuntu-latest")], {1: content})
        with patch.dict(os.environ, {"GITHUB_TOKEN": "fixture-token"}), patch.object(campaign, "GitHub", return_value=api) as client:
            result = self.resolve(dict(mode="resume", resume_run_id="123"))
        self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 2})
        client.assert_called_once_with("owner/repo", "fixture-token", timeout_seconds=180)

    def test_resume_of_resume_uses_source_sidecar_and_all_available_platforms(self):
        linux, _ = checkpoint(source_ref="a" * 40)
        windows, _ = checkpoint("windows-latest", count=3, source_ref="a" * 40)
        entries = [dict(id=1, name="results-ubuntu-latest"), dict(id=2, name="checkpoint-windows-latest-1-3"),
                   dict(id=3, name="compared-results-macos-latest"),
                   dict(id=4, name="results-macos-latest", expired=True)]
        api, _ = self.api(entries, {1: linux, 2: windows})
        result = self.resolve(dict(mode="resume", resume_run_id="123", platform="macos-latest"), api)
        self.assertEqual(result["source_ref"], "a" * 40)
        self.assertEqual(result["platforms"], ["ubuntu-latest", "windows-latest"])
        self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 2, "windows-latest": 3})

    def test_newer_attempt_checkpoint_beats_old_final_and_explicit_artifact_selects_one_platform(self):
        entries = [dict(id=9, name="checkpoint-ubuntu-latest-1-2", created_at="2026-02-02"),
                   dict(id=1, name="results-ubuntu-latest", created_at="2026-02-01"),
                   dict(id=6, name="checkpoint-windows-latest-1-1", created_at="2026-02-01"),
                   dict(id=3, name="checkpoint-windows-latest-1-2", created_at="2026-02-02")]
        selected = campaign.select_resume_artifacts(entries)
        self.assertEqual({platform: item["id"] for platform, item in selected.items()},
                         {"ubuntu-latest": 9, "windows-latest": 3})
        entries[1]["created_at"] = "2026-02-02"
        self.assertEqual(campaign.select_resume_artifacts(entries)["ubuntu-latest"]["id"], 1)
        selected = campaign.select_resume_artifacts(entries, "checkpoint-windows-latest-1-1")
        self.assertEqual(list(selected), ["windows-latest"])
        for name in ("compared-results-ubuntu-latest", "../results-ubuntu-latest", "missing"):
            with self.assertRaises(ValueError):
                campaign.select_resume_artifacts(entries, name)

    def test_changed_order_missing_records_and_tampered_metadata_are_rejected(self):
        def missing(files):
            files.pop(next(name for name in files if name.startswith("records/")))

        def reordered(files):
            files["results.json"][:2] = files["results.json"][:2][::-1]

        def changed_record(files):
            files[next(name for name in files if name.startswith("records/"))]["source_sha"] = "f" * 40

        for alter in (missing, reordered, changed_record,
                      lambda files: files["manifest.json"].update(fingerprint="bad"),
                      lambda files: files["status.json"].update(recorded=0),
                      lambda files: files["results.json"][0].update(semimajor_m=.1),
                      lambda files: files.update({"../manifest.json": {}})):
            with self.subTest(alter=alter):
                content, _ = checkpoint(alter=alter)
                with self.assertRaises(ValueError):
                    campaign.checkpoint_settings(content, "owner/repo", "d" * 40, "ubuntu-latest")

    def test_resume_requires_same_repository_completed_manual_timing_run(self):
        content, _ = checkpoint()
        for key, value in (("repository", {"full_name": "other/repo"}),
                           ("head_repository", {"full_name": "other/repo"}),
                           ("event", "pull_request"), ("status", "in_progress"),
                           ("path", ".github/workflows/references.yml")):
            api, run = self.api([dict(id=1, name="results-ubuntu-latest")], {1: content})
            run[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.resolve(dict(mode="resume", resume_run_id="123"), api)
            api.archive.assert_not_called()

    def test_inconsistent_platform_selections_are_not_combined(self):
        linux, _ = checkpoint(source_ref="a" * 40)
        windows, _ = checkpoint("windows-latest", programs=["acousticts"], source_ref="a" * 40)
        api, _ = self.api([dict(id=1, name="results-ubuntu-latest"), dict(id=2, name="results-windows-latest")],
                          {1: linux, 2: windows})
        with self.assertRaisesRegex(ValueError, "different configurations"):
            self.resolve(dict(mode="resume", resume_run_id="123"), api)

    def test_cli_writes_compact_outputs_and_resolved_configuration(self):
        scratch = ROOT / ".scratch/codex/dense-benchmark/tests"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as temporary:
            root = Path(temporary)
            config, output, github_output = root / "config.json", root / "resolved.json", root / "github-output"
            config.write_text(json.dumps(configuration()))
            environment = dict(CAMPAIGN_INPUTS=json.dumps({"mode": "prepare"}), GITHUB_REPOSITORY="owner/repo",
                               GITHUB_SHA="e" * 40, GITHUB_OUTPUT=str(github_output))
            with patch.dict(os.environ, environment), patch.object(sys, "argv", ["campaign.py", "--config", str(config),
                                                                                  "--output", str(output)]):
                campaign.main()
            values = dict(line.split("=", 1) for line in github_output.read_text().splitlines())
            self.assertEqual(set(values), set(campaign.OUTPUT_FIELDS))
            self.assertEqual(json.loads(values["configuration"]), json.loads(output.read_text())["configuration"])
            self.assertEqual(json.loads(values["platforms"]), ["ubuntu-latest"])
            self.assertEqual(values["resume_run_id"], "")
            self.assertEqual(values["mode"], "prepare")


if __name__ == "__main__":
    unittest.main()
