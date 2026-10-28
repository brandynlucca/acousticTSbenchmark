"""Editable campaign inputs and reconstruction of immutable timing artifacts."""

import copy
import io
import itertools
import json
import os
from pathlib import Path
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import Mock, patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import benchmark
import campaign
import resume_order
import refresh
import publish_progress as progress


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


def checkpoint(platform="ubuntu-latest", programs=None, repeats=3, count=2, source_ref=None, alter=None,
               case_order=None, profile="pilot"):
    config = configuration()
    if case_order is not None:
        config["case_order"] = case_order
    config["frequency"] = dict(start_hz=1000, stop_hz=3000, step_hz=1000)
    programs = programs or ["echosms", "acousticts"]
    cases = benchmark.build_cases(config, profile, programs, repeats)
    lock = {"sources": {name: {"sha": "a" * 40} for name in benchmark.CAPABILITIES}}
    identity = dict(configuration=config, sources=lock, profile=profile, cases=[case["case_id"] for case in cases],
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


def setup_checkpoint(source_ref="a" * 40, extra=None, parent=None):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("benchmark-source.json", json.dumps(dict(schema_version=1,
            repository="owner/repo", source_ref=source_ref, run_id=123,
            resumed_from_run_id=parent)))
        for name, value in (extra or {}).items():
            archive.writestr(name, json.dumps(value))
    return stream.getvalue()


class CampaignTests(unittest.TestCase):
    def api(self, entries, payloads):
        api = Mock()
        run = dict(repository=dict(full_name="owner/repo"), head_repository=dict(full_name="owner/repo"),
                   event="workflow_dispatch", status="completed", path=".github/workflows/benchmark.yml",
                   head_sha="d" * 40)
        api.request.side_effect = lambda method, path: ({"artifacts": entries} if "/artifacts?" in path else
            {"jobs": []} if "/jobs?" in path else run)
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
        self.assertEqual(config["case_order"], "boundary_major")
        self.assertEqual(cases[-1]["frequency_hz"], 300000)
        self.assertEqual(result["platforms"], ["ubuntu-latest"])

    def test_focused_sweeps_complete_each_combination_with_all_software_and_repetitions(self):
        inputs = dict(semimajor_m="0.1", aspect_ratios="10,1.01", incidences_deg="90,37",
                      boundaries="gas,rigid", software="acousticts,echosms", repetitions=3,
                      frequency_start_khz="1", frequency_end_khz="4", frequency_interval_khz="2")
        result = self.resolve(inputs)
        cases = benchmark.build_cases(result["configuration"], result["profile"])
        key = lambda case: (case["semimajor_m"], case["boundary"], case["aspect_ratio"], case["incidence_deg"])
        groups = [(group, list(items)) for group, items in itertools.groupby(cases, key)]
        self.assertEqual([group for group, _ in groups],
                         list(itertools.product([.1], ["gas", "rigid"], [10, 1.01], [90, 37])))
        for _, items in groups:
            frequencies = [(frequency, list(batch)) for frequency, batch in
                           itertools.groupby(items, lambda case: case["frequency_hz"])]
            self.assertEqual([frequency for frequency, _ in frequencies], [1000, 3000, 4000])
            for _, batch in frequencies:
                self.assertCountEqual([(case["software"], case["repetition"]) for case in batch],
                                      itertools.product(["acousticts", "echosms"], [1, 2, 3]))
        for choice in ("Frequency by frequency", "frequency_round_robin"):
            round_robin = self.resolve(dict(inputs, case_order=choice))
            interleaved = benchmark.build_cases(round_robin["configuration"], round_robin["profile"])
            self.assertEqual([case["frequency_hz"] for case in interleaved],
                             sorted(case["frequency_hz"] for case in cases))
            self.assertEqual({case["case_id"]: case for case in cases},
                             {case["case_id"]: case for case in interleaved})
        for choice in ("Complete each frequency sweep", "boundary_major"):
            selected = self.resolve(dict(inputs, case_order=choice))
            self.assertEqual(benchmark.build_cases(selected["configuration"], selected["profile"]), cases)

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
            {"mode": "restart"}, {"case_order": "unknown"}, {"case_order": []},
            {"frequency_end_khz": "1000000000", "frequency_interval_khz": ".001"}):
            with self.subTest(inputs=inputs), self.assertRaises(ValueError):
                self.resolve(inputs)

    def test_resume_recovers_actual_cli_selection_and_ignores_new_run_fields(self):
        content, original = checkpoint()
        api, _ = self.api([dict(id=1, name="results-ubuntu-latest")], {1: content})
        result = self.resolve(dict(mode="resume", resume_run_id="123", platform="invalid", software="invalid",
            repetitions=0, frequency_end_khz=-1, aspect_ratios="1", semimajor_m="NaN", case_order="invalid"), api)
        self.assertEqual(result["software"], "echosms,acousticts")
        self.assertEqual(result["repetitions"], 3)
        self.assertEqual(result["configuration"], original)
        self.assertEqual(result["profile"], "pilot")
        self.assertEqual(result["source_ref"], "d" * 40)
        self.assertEqual(result["planned_cases"], 18)
        self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 2})
        self.assertEqual(result["resume_artifacts"], {"ubuntu-latest": "results-ubuntu-latest"})

    def test_resume_preserves_saved_order_even_when_new_order_is_selected(self):
        for saved, requested in (("frequency_round_robin", "Complete each frequency sweep"),
                                 ("boundary_major", "Frequency by frequency")):
            with self.subTest(saved=saved):
                content, original = checkpoint(case_order=saved, source_ref="a" * 40)
                api, _ = self.api([dict(id=1, name="results-ubuntu-latest")], {1: content})
                result = self.resolve(dict(mode="resume", resume_run_id="123", case_order=requested), api)
                self.assertEqual(result["configuration"], original)
                self.assertEqual(result["source_ref"], "a" * 40)
                self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 2})
                self.assertEqual(result["resume_case_order"], "")

    def test_reference_campaign_reads_active_checkpoint_without_resuming(self):
        content, original = checkpoint(case_order="boundary_major", source_ref="a" * 40)
        api, run = self.api([dict(id=1, name="checkpoint-ubuntu-latest-1-1")], {1: content})
        run["status"] = "in_progress"
        result = campaign.reference_campaign(123, "owner/repo", api)
        self.assertEqual(result, dict(configuration=original, profile="pilot", source_ref="a" * 40))
        self.assertTrue(all(call.args[0] == "GET" for call in api.request.call_args_list))
        with self.assertRaisesRegex(ValueError, "completed"):
            self.resolve(dict(mode="resume", resume_run_id="123"), api)
        run["repository"]["full_name"] = "different/repo"
        with self.assertRaisesRegex(ValueError, "this repository"):
            campaign.reference_campaign(123, "owner/repo", api)

    def test_explicit_resume_order_preserves_original_settings_for_validated_migration(self):
        content, original = checkpoint(case_order="frequency_round_robin", source_ref="a" * 40)
        api, _ = self.api([dict(id=1, name="results-ubuntu-latest")], {1: content})
        for choice, expected in (("Complete each frequency sweep", "boundary_major"),
                                 ("Frequency by frequency", "frequency_round_robin"), ("Keep saved order", "")):
            result = self.resolve(dict(mode="resume", resume_run_id="123", resume_case_order=choice), api)
            self.assertEqual(result["resume_case_order"], expected)
            self.assertEqual(result["configuration"], original)
            self.assertEqual(result["source_ref"], "a" * 40)
            self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 2})
        for choice in ("unknown", [], None):
            with self.subTest(choice=choice), self.assertRaisesRegex(ValueError, "resume_case_order"):
                self.resolve(dict(mode="resume", resume_run_id="123", resume_case_order=choice), api)

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

    def test_selected_resume_platform_ignores_other_artifacts_and_unstarted_jobs(self):
        linux, original = checkpoint(source_ref="a" * 40)
        entries = [dict(id=1, name="results-ubuntu-latest"), dict(id=2, name="results-windows-latest")]
        api, _ = self.api(entries, {1: linux, 2: b"broken Windows artifact"})
        previous_request = api.request.side_effect
        api.request.side_effect = lambda method, path: (dict(jobs=[
            dict(name=f"benchmark ({platform})", steps=None) for platform in campaign.PLATFORMS])
            if "/jobs?" in path else previous_request(method, path))
        result = self.resolve(dict(mode="resume", resume_run_id="123",
                                   continuation_platform="ubuntu-latest"), api)
        self.assertEqual(result["platforms"], ["ubuntu-latest"])
        self.assertEqual(result["configuration"], original)
        self.assertEqual(result["source_ref"], "a" * 40)
        self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 2})
        self.assertEqual(result["resume_artifacts"], {"ubuntu-latest": "results-ubuntu-latest"})
        api.archive.assert_called_once_with(1)

    def test_selected_resume_platform_requires_its_own_available_checkpoint(self):
        linux, _ = checkpoint()
        entries = [dict(id=1, name="results-ubuntu-latest"),
                   dict(id=2, name="results-windows-latest", expired=True)]
        api, _ = self.api(entries, {1: linux})
        for selection in ("windows-latest", "macos-latest"):
            with self.subTest(selection=selection), self.assertRaisesRegex(ValueError, selection):
                self.resolve(dict(mode="resume", resume_run_id="123", continuation_platform=selection), api)
        api.archive.assert_not_called()
        with self.assertRaisesRegex(ValueError, "resume_artifact"):
            self.resolve(dict(mode="resume", resume_run_id="123", continuation_platform="windows-latest",
                              resume_artifact="results-ubuntu-latest"), api)
        for selection in (None, [], "all", "ubuntu"):
            with self.subTest(selection=selection), self.assertRaisesRegex(ValueError, "continuation_platform"):
                self.resolve(dict(mode="resume", resume_run_id="123", continuation_platform=selection), api)
        api.archive.assert_not_called()

    def test_explicit_keep_saved_platforms_preserves_all_platforms(self):
        linux, _ = checkpoint()
        windows, _ = checkpoint("windows-latest", count=3)
        api, _ = self.api([dict(id=1, name="results-ubuntu-latest"),
                          dict(id=2, name="results-windows-latest")], {1: linux, 2: windows})
        result = self.resolve(dict(mode="resume", resume_run_id="123",
                                   continuation_platform="Keep saved platforms"), api)
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

    def test_resume_keeps_queued_platforms_without_repeating_saved_cases(self):
        content, original = checkpoint(source_ref="a" * 40)
        api, run = self.api([dict(id=1, name="results-ubuntu-latest")], {1: content})
        original_request = api.request.side_effect
        jobs = [dict(name="benchmark (ubuntu-latest)", steps=[dict(name="Benchmark chunk 1", status="completed", conclusion="success")]),
                dict(name="benchmark (windows-latest)", steps=None),
                dict(name="benchmark (macos-latest)", steps=[dict(name="Benchmark chunk 1", status="completed", conclusion="skipped")]),
                dict(name="references / references (0)", steps=[])]
        api.request.side_effect = lambda method, path: {"jobs": jobs} if "/jobs?" in path else original_request(method, path)
        result = self.resolve(dict(mode="resume", resume_run_id="123"), api)
        self.assertEqual(result["platforms"], list(campaign.PLATFORMS))
        self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 2, "windows-latest": 0, "macos-latest": 0})
        self.assertEqual(result["resume_artifacts"], {"ubuntu-latest": "results-ubuntu-latest"})
        self.assertEqual(result["source_ref"], "a" * 40)
        self.assertEqual(result["configuration"], original)
        result = self.resolve(dict(mode="resume", resume_run_id="123", resume_artifact="results-ubuntu-latest"), api)
        self.assertEqual(result["platforms"], ["ubuntu-latest"])

    def test_resume_does_not_restart_platforms_with_missing_measurements(self):
        content, _ = checkpoint()
        for name in ("Benchmark chunk 1", "Benchmark remaining campaign budget", "Restore previous checkpoints"):
            api, _ = self.api([dict(id=1, name="results-ubuntu-latest")], {1: content})
            original_request = api.request.side_effect
            jobs = [dict(name="benchmark (windows-latest)", steps=[dict(name=name, status="completed", conclusion="cancelled")])]
            api.request.side_effect = lambda method, path: {"jobs": jobs} if "/jobs?" in path else original_request(method, path)
            with self.subTest(step=name), self.assertRaisesRegex(ValueError, "no usable checkpoint"):
                self.resolve(dict(mode="resume", resume_run_id="123"), api)

    def test_resume_preserves_finished_platforms_and_retries_setup_only_platform(self):
        ubuntu, original = checkpoint(source_ref="a" * 40)
        windows, _ = checkpoint(platform="windows-latest", source_ref="a" * 40, count=4)
        entries = [dict(id=1, name="results-ubuntu-latest"), dict(id=2, name="results-windows-latest"),
                   dict(id=3, name="results-macos-latest")]
        api, _ = self.api(entries, {1: ubuntu, 2: windows, 3: setup_checkpoint()})
        original_request = api.request.side_effect
        mac_job = dict(name="benchmark (macos-latest)", steps=[
            dict(name="Prepare pinned software", status="completed", conclusion="failure"),
            dict(name="Restore previous checkpoints", status="completed", conclusion="skipped"),
            dict(name="Benchmark chunk 1", status="completed", conclusion="skipped")])
        api.request.side_effect = lambda method, path: {"jobs": [mac_job]} if "/jobs?" in path else original_request(method, path)
        result = self.resolve(dict(mode="resume", resume_run_id="123"), api)
        self.assertEqual(result["platforms"], list(campaign.PLATFORMS))
        self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 2, "windows-latest": 4, "macos-latest": 0})
        self.assertEqual(set(result["resume_artifacts"]), {"ubuntu-latest", "windows-latest"})
        self.assertEqual(result["configuration"], original)
        self.assertEqual(result["source_ref"], "a" * 40)
        for name in ("Restore previous checkpoints", "Benchmark chunk 1"):
            mac_job["steps"] = [dict(name=name, status="completed", conclusion="failure")]
            with self.subTest(step=name), self.assertRaisesRegex(ValueError, "no usable checkpoint"):
                self.resolve(dict(mode="resume", resume_run_id="123"), api)

    def test_setup_only_artifact_cannot_hide_partial_records_or_replace_configuration(self):
        content, _ = checkpoint(source_ref="a" * 40)
        entries = [dict(id=1, name="results-ubuntu-latest"), dict(id=2, name="results-macos-latest")]
        for bad in (setup_checkpoint(extra={"records/" + "b" * 64 + ".json": dict(status="ok")}),
                    setup_checkpoint(source_ref="f" * 40)):
            api, _ = self.api(entries, {1: content, 2: bad})
            original_request = api.request.side_effect
            api.request.side_effect = lambda method, path: {"jobs": [dict(name="benchmark (macos-latest)", steps=[])]} if "/jobs?" in path else original_request(method, path)
            with self.assertRaises(ValueError):
                self.resolve(dict(mode="resume", resume_run_id="123"), api)
        api, _ = self.api(entries, {1: content, 2: setup_checkpoint()})
        with self.assertRaisesRegex(ValueError, "setup metadata only"):
            self.resolve(dict(mode="resume", resume_run_id="123", resume_artifact="results-macos-latest"), api)

    def test_setup_only_resume_proves_parent_platform_was_queued_without_measurements(self):
        content, _ = checkpoint(source_ref="a" * 40)

        def parent_provenance(files):
            files["benchmark-source.json"].update(run_id=122, resumed_from_run_id=None)

        parent_content, _ = checkpoint(source_ref="a" * 40, alter=parent_provenance)
        entries = [dict(id=1, name="results-ubuntu-latest"), dict(id=2, name="results-macos-latest")]
        api, run = self.api(entries, {1: content, 2: setup_checkpoint(parent=122), 3: parent_content})
        current_job = dict(name="benchmark (macos-latest)", steps=[
            dict(name="Prepare pinned software", status="completed", conclusion="failure"),
            dict(name="Restore previous checkpoints", status="completed", conclusion="skipped")])
        parent_job = dict(name="benchmark (macos-latest)", status="completed", conclusion="cancelled", steps=[])
        parent_artifacts = [dict(id=3, name="checkpoint-ubuntu-latest-1-5")]

        def request(method, path):
            if path.startswith("/actions/runs/122/"):
                return {"jobs": [parent_job]} if "/jobs?" in path else {"artifacts": parent_artifacts}
            if "/jobs?" in path:
                return {"jobs": [current_job]}
            if "/artifacts?" in path:
                return {"artifacts": entries}
            return run

        api.request.side_effect = request
        result = self.resolve(dict(mode="resume", resume_run_id="123"), api)
        self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 2, "macos-latest": 0})
        self.assertEqual(result["resume_artifacts"], {"ubuntu-latest": "results-ubuntu-latest"})
        # A previous platform artifact must prevent a reset even if that
        # artifact has expired and restoration never began in the latest run.
        for expired in (False, True):
            parent_artifacts.append(dict(id=4, name="results-macos-latest", expired=expired))
            with self.subTest(expired=expired), self.assertRaisesRegex(ValueError, "parent run 122 may contain saved measurements"):
                self.resolve(dict(mode="resume", resume_run_id="123"), api)
            parent_artifacts.pop()
        parent_job["steps"] = [dict(name="Benchmark chunk 1", status="completed", conclusion="cancelled")]
        with self.assertRaisesRegex(ValueError, "parent run 122 may contain saved measurements"):
            self.resolve(dict(mode="resume", resume_run_id="123"), api)

    def test_setup_only_parent_proof_rejects_ambiguous_or_cyclic_lineage(self):
        source = dict(schema_version=1, repository="owner/repo", source_ref="a" * 40,
                      run_id=123, resumed_from_run_id=122)
        content, _ = checkpoint(source_ref="a" * 40)
        api, run = self.api([dict(id=1, name="results-ubuntu-latest")], {1: content})
        original_request = api.request.side_effect
        api.request.side_effect = lambda method, path: {"jobs": [dict(name="benchmark (macos-latest)", steps=[])]} if "/jobs?" in path else original_request(method, path)
        with self.assertRaisesRegex(ValueError, "parent source metadata"):
            campaign.prove_unstarted_ancestry(source, "macos-latest", "owner/repo", api)
        source["resumed_from_run_id"] = 123
        with self.assertRaisesRegex(ValueError, "continuation ancestry"):
            campaign.prove_unstarted_ancestry(source, "macos-latest", "owner/repo", api)

    def test_cancelled_resume_recovers_atomic_records_saved_since_last_table_export(self):
        def ahead(files):
            row = files["results.json"][2]
            files["records/" + row["case_id"] + ".json"] = dict(case_id=row["case_id"],
                source_sha="a" * 40, status="ok", elapsed_seconds=.3, ts_db=-42)
        content, _ = checkpoint(alter=ahead)
        api, run = self.api([dict(id=1, name="results-ubuntu-latest")], {1: content})
        with self.assertRaisesRegex(ValueError, "every recorded case"):
            self.resolve(dict(mode="resume", resume_run_id="123"), api)
        run["conclusion"] = "cancelled"
        result = self.resolve(dict(mode="resume", resume_run_id="123"), api)
        self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 3})
        def wrong_source(files):
            ahead(files)
            row = files["results.json"][2]
            files["records/" + row["case_id"] + ".json"]["source_sha"] = "f" * 40
        content, _ = checkpoint(alter=wrong_source)
        api.archive.return_value = content
        api.archive.side_effect = None
        with self.assertRaisesRegex(ValueError, "pinned source"):
            self.resolve(dict(mode="resume", resume_run_id="123"), api)

    def test_cancelled_resume_recovers_table_export_ahead_of_summary(self):
        def stale_summary(files):
            files["status.json"]["recorded"] = 1
        content, _ = checkpoint(alter=stale_summary)
        api, run = self.api([dict(id=1, name="results-ubuntu-latest")], {1: content})
        with self.assertRaisesRegex(ValueError, "every recorded case"):
            self.resolve(dict(mode="resume", resume_run_id="123"), api)
        run["conclusion"] = "cancelled"
        result = self.resolve(dict(mode="resume", resume_run_id="123"), api)
        self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 2})

    def test_failed_or_cancelled_export_recovers_missing_summary_from_preserved_records(self):
        content, _ = checkpoint(alter=lambda files: files.pop("status.json"))
        with self.assertRaisesRegex(ValueError, "lacks its manifest, status"):
            campaign.read_checkpoint(content)
        for conclusion in ("failure", "cancelled"):
            with self.subTest(conclusion=conclusion):
                api, run = self.api([dict(id=1, name="results-ubuntu-latest")], {1: content})
                run["conclusion"] = conclusion
                result = self.resolve(dict(mode="resume", resume_run_id="123"), api)
                self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 2})
        values, records = campaign.read_checkpoint(content, allow_unexported=True)
        self.assertEqual(values["status.json"], dict(state="interrupted", planned=18, recorded=2,
                                                   outcomes=dict(ok=2, pending=16)))
        self.assertEqual(len(records), 2)

    def test_missing_summary_cannot_bypass_manifest_case_or_record_validation(self):
        mutations = (
            lambda files: files.pop("results.json"),
            lambda files: files["results.json"].pop(),
            lambda files: files["results.json"][0].update(elapsed_seconds=999),
            lambda files: files.pop(next(name for name in files if name.startswith("records/"))),
            lambda files: files["manifest.json"].update(fingerprint="invalid"),
        )
        for mutation in mutations:
            def interrupted(files):
                files.pop("status.json")
                mutation(files)
            content, _ = checkpoint(alter=interrupted)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                campaign.checkpoint_settings(content, "owner/repo", "a" * 40, "ubuntu-latest",
                                             allow_unexported=True)

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


class ResumeOrderTests(unittest.TestCase):
    def setUp(self):
        scratch = ROOT / ".scratch/codex/dense-benchmark/tests"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def saved(self, alter=None):
        content, configuration = checkpoint(case_order="frequency_round_robin", profile="full", repeats=1,
                                             count=20, source_ref="a" * 40, alter=alter)
        values, records = campaign.read_checkpoint(content)
        for name, value in values.items():
            benchmark.atomic_json(self.directory / name, value)
        for key, record in records.items():
            benchmark.atomic_json(self.directory / "records" / f"{key}.json", record)
        return configuration

    def apply(self, config, order="boundary_major"):
        return resume_order.change_order(self.directory, config, "full", ["echosms", "acousticts"], 1, order)

    def test_order_change_preserves_records_references_and_source_identity(self):
        def matched(files):
            files["results.json"][0].update(reference_id="qualified-reference", delta_ts_db=1.25)
        config = self.saved(matched)
        before = benchmark.read_json(self.directory / "manifest.json")
        rows = {row["case_id"]: row for row in benchmark.read_json(self.directory / "results.json")}
        raw_records = {path: path.read_bytes() for path in (self.directory / "records").glob("*.json")}
        revised, count = self.apply(config)
        after = benchmark.read_json(self.directory / "manifest.json")
        self.assertEqual(revised, dict(config, case_order="boundary_major"))
        self.assertEqual(count, 20)
        self.assertNotEqual(before["cases"], after["cases"])
        self.assertEqual({row["case_id"]: row for row in benchmark.read_json(self.directory / "results.json")}, rows)
        self.assertEqual({path: path.read_bytes() for path in raw_records}, raw_records)
        for key in campaign.IDENTITY_FIELDS:
            if key not in ("cases", "configuration"):
                self.assertEqual(after[key], before[key])
        self.assertEqual(after["fingerprint"], benchmark.fingerprint({key: after[key] for key in campaign.IDENTITY_FIELDS}))
        self.assertEqual(after["order_changes"][0]["previous_fingerprint"], before["fingerprint"])
        self.assertEqual(after["order_changes"][0]["preserved_records"], 20)
        # The next continuation must accept the new ordering without allowing
        # any changes to source, numerical controls, or completed measurements.
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w") as archive:
            for path in self.directory.rglob("*.json"):
                archive.writestr(path.relative_to(self.directory).as_posix(), path.read_bytes())
        settings = campaign.checkpoint_settings(stream.getvalue(), "owner/repo", "b" * 40, "ubuntu-latest")
        self.assertEqual(settings["source_ref"], "a" * 40)
        self.assertEqual(settings["configuration"], revised)
        self.assertEqual(settings["recorded_cases"], 20)
        self.apply(revised)
        self.assertEqual(benchmark.read_json(self.directory / "manifest.json"), after)

    def test_cancelled_record_newer_than_export_is_preserved(self):
        def unexported(files):
            row = files["results.json"][20]
            files[f"records/{row['case_id']}.json"] = dict(case_id=row["case_id"], status="numerical_error",
                                                          elapsed_seconds=2., ts_db=None, source_sha="a" * 40)
        config = self.saved(unexported)
        _, count = self.apply(config)
        self.assertEqual(count, 21)
        status = benchmark.read_json(self.directory / "status.json")
        self.assertEqual(status["recorded"], 21)
        failures = [row for row in benchmark.read_json(self.directory / "results.json") if row["status"] == "numerical_error"]
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["elapsed_seconds"], 2.)
        self.assertIsNone(failures[0]["delta_ts_db"])

    def test_invalid_records_or_settings_are_rejected_before_any_writes(self):
        config = self.saved()
        snapshot = lambda: {path: path.read_bytes() for path in self.directory.rglob("*") if path.is_file()}
        before = snapshot()
        with self.assertRaisesRegex(ValueError, "manifest"):
            self.apply(dict(config, warmup=False))
        self.assertEqual(snapshot(), before)
        record_path = next((self.directory / "records").glob("*.json"))
        record = benchmark.read_json(record_path)
        record["elapsed_seconds"] = 999
        benchmark.atomic_json(record_path, record)
        before = snapshot()
        with self.assertRaisesRegex(ValueError, "measurement"):
            self.apply(config)
        self.assertEqual(snapshot(), before)
        (self.directory / "running.lock").write_text("123")
        with self.assertRaisesRegex(ValueError, "active"):
            self.apply(config)

    def test_queued_platform_uses_the_selected_order_without_fabricated_records(self):
        config = dict(configuration(), case_order="frequency_round_robin")
        revised, count = self.apply(config)
        self.assertEqual(revised, dict(config, case_order="boundary_major"))
        self.assertEqual(count, 0)
        self.assertEqual(list(self.directory.iterdir()), [])


class RefreshTests(unittest.TestCase):
    api = CampaignTests.api
    resolve = CampaignTests.resolve

    def single_refresh(self):
        content, original = checkpoint(programs=["echosms", "acousticts", "prol_spheroid"],
                                       count=6, source_ref="a" * 40, case_order="frequency_round_robin")
        api, _ = self.api([dict(id=1, name="results-ubuntu-latest")], {1: content})
        with patch.dict(os.environ, GITHUB_RUN_ID="124"):
            result = self.resolve(dict(mode="refresh", resume_run_id="123", refresh_software="acousticts",
                semimajor_m="invalid", boundaries="invalid", repetitions=0, software="invalid"), api)
        return result, content, original

    @staticmethod
    def archive_files(files):
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            for name, value in files.items():
                archive.writestr(name, json.dumps(value))
        return output.getvalue()

    def test_refresh_replaces_only_selected_software_and_preserves_prediction_inputs(self):
        result, content, original = self.single_refresh()
        self.assertEqual((result["mode"], result["software"], result["repetitions"], result["profile"]),
                         ("refresh", "acousticts", 3, "pilot"))
        self.assertEqual(result["source_ref"], "e" * 40)
        self.assertEqual(result["planned_cases"], 9)
        self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 0})
        self.assertEqual(result["resume_artifact_run_ids"], {"ubuntu-latest": 124})
        expected = dict(original, software=["acousticts"], repetitions=3, case_order="boundary_major")
        self.assertEqual(result["configuration"], expected)
        saved = result["_retained_payloads"]["ubuntu-latest"]
        self.assertEqual(len(saved["rows"]), 4)
        self.assertEqual({row["software"] for row in saved["rows"]}, {"echosms", "prol_spheroid"})
        old_rows = {row["case_id"]: row for row in campaign.read_checkpoint(content)[0]["results.json"]}
        for row in saved["rows"]:
            self.assertEqual(row, dict(old_rows[row["case_id"]], measurement_role="retained", retained_from_run_id=123))
        evidence = saved["provenance"][0]
        self.assertEqual(evidence["manifest"], campaign.read_checkpoint(content)[0]["manifest.json"])
        self.assertEqual(evidence["run_id"], 123)

    def test_retained_environment_evidence_deduplicates_runtime_without_losing_invocations(self):
        shared = dict(platform="Linux", runtime=dict(source_shas=dict(acousticts="a" * 40),
                                                     versions=dict(R="4.5.2")))
        files = {f"environments/invocation-{index}.json": dict(shared, started_at=f"2026-10-24T00:0{index}:00Z",
                    references_fingerprint=str(index) * 64) for index in range(3)}
        files["environments/different-build.json"] = dict(shared, runtime=dict(versions=dict(R="4.5.1")))
        evidence = refresh._environments(self.archive_files(files))
        self.assertEqual(set(evidence["invocations"]), {"invocation-0", "invocation-1", "invocation-2", "different-build"})
        self.assertEqual(len(evidence["evidence"]), 2)
        for index in range(3):
            invocation = evidence["invocations"][f"invocation-{index}"]
            self.assertEqual(invocation["started_at"], f"2026-10-24T00:0{index}:00Z")
            self.assertEqual(invocation["references_fingerprint"], str(index) * 64)
            self.assertEqual(evidence["evidence"][invocation["environment_fingerprint"]], shared)

    def ancestry_api(self, mismatch=False):
        def current_source(files):
            files["benchmark-source.json"].update(run_id=123, resumed_from_run_id=122)
        linux, _ = checkpoint(source_ref="a" * 40, count=4, alter=current_source,
                              case_order="boundary_major")
        windows, _ = checkpoint(platform="windows-latest", source_ref="a" * 40, count=6,
                                case_order="frequency_round_robin", profile="full" if mismatch else "pilot")
        api, run = self.api([], {1: linux, 2: windows})
        jobs = {123: [dict(name="benchmark (ubuntu-latest)", steps=[]),
                      dict(name="benchmark (windows-latest)", steps=None)],
                122: [dict(name="benchmark (windows-latest)", steps=[])]}
        artifacts = {123: [dict(id=1, name="results-ubuntu-latest")],
                     122: [dict(id=2, name="results-windows-latest")]}
        def request(method, path):
            run_id = int(path.split("/")[3])
            if "/jobs?" in path:
                return dict(jobs=jobs[run_id])
            if "/artifacts?" in path:
                return dict(artifacts=artifacts[run_id])
            return run
        api.request.side_effect = request
        return api

    def test_refresh_recovers_queued_windows_comparisons_from_parent(self):
        result = self.resolve(dict(mode="refresh", resume_run_id="123"), self.ancestry_api())
        self.assertEqual(result["platforms"], ["ubuntu-latest", "windows-latest"])
        self.assertEqual(result["retained_by_platform"], {"ubuntu-latest": 2, "windows-latest": 3})
        self.assertEqual({row["retained_from_run_id"] for row in result["_retained_payloads"]["windows-latest"]["rows"]}, {122})
        self.assertEqual({row["retained_from_run_id"] for row in result["_retained_payloads"]["ubuntu-latest"]["rows"]}, {123})

    def test_refresh_single_platform_does_not_traverse_omitted_platform_ancestry(self):
        api = self.ancestry_api(mismatch=True)
        result = self.resolve(dict(mode="refresh", resume_run_id="123",
                                   continuation_platform="ubuntu-latest"), api)
        self.assertEqual(result["platforms"], ["ubuntu-latest"])
        self.assertEqual(result["retained_by_platform"], {"ubuntu-latest": 2})
        self.assertEqual(set(result["_retained_payloads"]), {"ubuntu-latest"})
        api.archive.assert_called_once_with(1)
        self.assertFalse(any("/runs/122" in call.args[1] for call in api.request.call_args_list))

    def test_refresh_platform_filter_precedes_validation_and_rejects_missing_checkpoint(self):
        content, _ = checkpoint(count=4)
        entries = [dict(id=1, name="results-ubuntu-latest"), dict(id=2, name="results-windows-latest")]
        api, _ = self.api(entries, {1: content, 2: b"corrupt unrelated checkpoint"})
        result = self.resolve(dict(mode="refresh", resume_run_id="123",
                                   continuation_platform="ubuntu-latest"), api)
        self.assertEqual(result["platforms"], ["ubuntu-latest"])
        self.assertEqual(result["retained_by_platform"], {"ubuntu-latest": 2})
        api.archive.assert_called_once_with(1)
        api.archive.reset_mock()
        with self.assertRaisesRegex(ValueError, "macos-latest"):
            self.resolve(dict(mode="refresh", resume_run_id="123",
                              continuation_platform="macos-latest"), api)
        api.archive.assert_not_called()

    def test_refresh_refuses_ancestral_configuration_mismatch(self):
        with self.assertRaisesRegex(ValueError, "different physical campaign"):
            self.resolve(dict(mode="refresh", resume_run_id="123"), self.ancestry_api(mismatch=True))

    def test_refresh_traverses_intermediate_queued_runs_without_replacing_newer_rows(self):
        api = self.ancestry_api()
        original_request, original_archive = api.request.side_effect, api.archive.side_effect
        def parent_link(files):
            files["benchmark-source.json"].update(run_id=122, resumed_from_run_id=121)
        intermediate, _ = checkpoint(source_ref="a" * 40, count=2, alter=parent_link)
        def request(method, path):
            if path.startswith("/actions/runs/122/"):
                if "/artifacts?" in path:
                    return dict(artifacts=[dict(id=3, name="results-ubuntu-latest")])
                if "/jobs?" in path:
                    return dict(jobs=[dict(name="benchmark (ubuntu-latest)", steps=[]),
                                      dict(name="benchmark (windows-latest)", steps=None)])
            if path.startswith("/actions/runs/121"):
                return original_request(method, path.replace("/121", "/122", 1))
            return original_request(method, path)
        api.request.side_effect = request
        api.archive.side_effect = lambda identity: intermediate if identity == 3 else original_archive(identity)
        result = self.resolve(dict(mode="refresh", resume_run_id="123"), api)
        self.assertEqual(result["retained_by_platform"], {"ubuntu-latest": 2, "windows-latest": 3})
        self.assertEqual({row["retained_from_run_id"] for row in result["_retained_payloads"]["windows-latest"]["rows"]}, {121})
        self.assertEqual({row["retained_from_run_id"] for row in result["_retained_payloads"]["ubuntu-latest"]["rows"]}, {123})

    def test_baselines_round_trip_and_tampering_is_rejected(self):
        result, _, _ = self.single_refresh()
        lock = {"sources": {name: {"sha": "f" * 40} for name in benchmark.CAPABILITIES}}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            refresh.write_baselines(result, root, "owner/repo", 124, sources=lock)
            files = {name: benchmark.read_json(root / "ubuntu-latest" / name)
                     for name in ("refresh-campaign.json", "retained-results.json")}
        settings = refresh.baseline_settings(self.archive_files(files), "owner/repo", 124, "ubuntu-latest")
        self.assertEqual(settings["software"], "acousticts")
        self.assertEqual(settings["planned_cases"], 9)
        self.assertEqual(settings["recorded_cases"], 0)
        for altered in ("row", "origin", "source"):
            changed = copy.deepcopy(files)
            if altered == "row":
                changed["retained-results.json"]["rows"][0]["elapsed_seconds"] = 999
            elif altered == "origin":
                changed["refresh-campaign.json"]["run_id"] = 123
            else:
                changed["refresh-campaign.json"]["sources"]["sources"]["acousticts"]["sha"] = "0" * 40
            with self.subTest(altered=altered), self.assertRaises(ValueError):
                refresh.baseline_settings(self.archive_files(changed), "owner/repo", 124, "ubuntu-latest")

    def test_legacy_retained_size_limit_is_shared_by_baseline_and_checkpoint_readers(self):
        result, _, _ = self.single_refresh()
        lock = {"sources": {name: {"sha": "f" * 40} for name in benchmark.CAPABILITIES}}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            refresh.write_baselines(result, root, "owner/repo", 124, sources=lock)
            files = {name: benchmark.read_json(root / "ubuntu-latest" / name)
                     for name in ("refresh-campaign.json", "retained-results.json")}
        files["retained-results.json"]["provenance"][0]["legacy_reference_plan"] = "x" * 200000
        files["refresh-campaign.json"]["retained_fingerprint"] = benchmark.fingerprint(files["retained-results.json"])
        baseline = self.archive_files(files)
        checkpoint_content = self.archive_files({"manifest.json": {}, "status.json": {}, "results.json": [],
            "retained-results.json": files["retained-results.json"]})
        with patch.object(progress, "MAX_JSON_BYTES", 100000), patch.object(progress, "MAX_RETAINED_BYTES", 300000):
            settings = refresh.baseline_settings(baseline, "owner/repo", 124, "ubuntu-latest")
            self.assertEqual(settings["recorded_cases"], 0)
            restored, _ = campaign.read_checkpoint(checkpoint_content)
            self.assertEqual(restored["retained-results.json"], files["retained-results.json"])
        with patch.object(progress, "MAX_RETAINED_BYTES", 200000):
            with self.assertRaisesRegex(ValueError, "Invalid refresh baseline file"):
                refresh.baseline_settings(baseline, "owner/repo", 124, "ubuntu-latest")
            with self.assertRaisesRegex(ValueError, "supported size"):
                campaign.read_checkpoint(checkpoint_content)

    def test_resume_accepts_completed_timing_plus_queued_refresh_baseline(self):
        result, content, _ = self.single_refresh()
        result["platforms"].append("windows-latest")
        result["_retained_payloads"]["windows-latest"] = copy.deepcopy(result["_retained_payloads"]["ubuntu-latest"])
        lock = {"sources": {name: {"sha": "f" * 40} for name in benchmark.CAPABILITIES}}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            refresh.write_baselines(result, root, "owner/repo", 124, sources=lock)
            windows = {name: benchmark.read_json(root / "windows-latest" / name)
                       for name in ("refresh-campaign.json", "retained-results.json")}
            retained = benchmark.read_json(root / "ubuntu-latest/retained-results.json")
        cases = benchmark.build_cases(result["configuration"], result["profile"], ["acousticts"], 3)
        original_manifest = campaign.read_checkpoint(content)[0]["manifest.json"]
        identity = dict(campaign.manifest_identity(original_manifest), configuration=result["configuration"],
                        sources=lock, cases=[case["case_id"] for case in cases],
                        retained_fingerprint=benchmark.fingerprint(retained))
        record = dict(case_id=cases[0]["case_id"], status="ok", source_sha="f" * 40, elapsed_seconds=.05, ts_db=-40)
        files = {"manifest.json": dict(identity, fingerprint=benchmark.fingerprint(identity)),
                 "results.json": benchmark.summarize(cases, {record["case_id"]: record}, {}),
                 "status.json": dict(state="budget_paused", planned=len(cases), recorded=1),
                 "retained-results.json": retained, f"records/{record['case_id']}.json": record,
                 "benchmark-source.json": dict(schema_version=1, repository="owner/repo", source_ref="e" * 40)}
        entries = [dict(id=1, name="results-ubuntu-latest"), dict(id=2, name="retained-windows-latest")]
        api, _ = self.api(entries, {1: self.archive_files(files), 2: self.archive_files(windows)})
        resumed = self.resolve(dict(mode="resume", resume_run_id="124"), api)
        self.assertEqual(resumed["software"], "acousticts")
        self.assertEqual(resumed["configuration"], result["configuration"])
        self.assertEqual(resumed["recorded_by_platform"], {"ubuntu-latest": 1, "windows-latest": 0})
        self.assertEqual(resumed["resume_artifact_run_ids"], {"ubuntu-latest": 124, "windows-latest": 124})

    def test_resumed_setup_baseline_tracks_current_run_without_changing_saved_measurements(self):
        result, _, _ = self.single_refresh()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            refresh.write_baselines(result, root, "owner/repo", 124)
            directory = root / "benchmark-results"
            for name in ("refresh-campaign.json", "retained-results.json"):
                benchmark.atomic_json(directory / name, benchmark.read_json(root / "ubuntu-latest" / name))
            retained_before = (directory / "retained-results.json").read_bytes()
            workflow = (ROOT / ".github/workflows/benchmark.yml").read_text(encoding="utf-8")
            block = workflow.split("      - name: Record benchmark source provenance\n", 1)[1]
            source = textwrap.dedent(block.split("<<'PY'\n", 1)[1].split("\n          PY", 1)[0])
            environment = dict(RUNNER_TEMP=str(root), SOURCE_REF="e" * 40, GITHUB_REPOSITORY="owner/repo",
                               GITHUB_WORKFLOW_REF="owner/repo/benchmark.yml@main", GITHUB_SHA="e" * 40,
                               GITHUB_RUN_ID="125", RESUME_RUN_ID="124")
            with patch.dict(os.environ, environment), patch("subprocess.check_output", return_value="e" * 40 + "\n"):
                exec(compile(source, "record-source", "exec"), {})
            files = {path.name: benchmark.read_json(path) for path in directory.glob("*.json")}
            settings = refresh.baseline_settings(self.archive_files(files), "owner/repo", 125, "ubuntu-latest")
            self.assertEqual(settings["software"], "acousticts")
            self.assertEqual(settings["recorded_cases"], 0)
            self.assertEqual(files["refresh-campaign.json"]["baseline_origin_run_id"], 124)
            self.assertEqual(files["refresh-campaign.json"]["resumed_from_run_id"], 124)
            self.assertEqual((directory / "retained-results.json").read_bytes(), retained_before)


class ExtendTests(unittest.TestCase):
    api = CampaignTests.api
    archive_files = staticmethod(RefreshTests.archive_files)

    def setUp(self):
        self.lock = {"sources": {name: {"sha": "a" * 40} for name in benchmark.CAPABILITIES}}
        self.runtime = dict(source_shas={name: "a" * 40 for name in benchmark.CAPABILITIES},
            environment_compatibility=dict(python_packages="numpy==2.2.6\nscipy==1.15.3\npandas==2.3.0\nmatplotlib==3.10.3\n",
                prol_compiler="GNU Fortran 14.2.0", prol_flags=["-O2"], r_packages={"R": "4.5.2"},
                julia_environments={"liquid_spheroid": {"Manifest.toml": "locked Julia dependencies"}}))

    def source(self, alter=None, platform="ubuntu-latest"):
        def evidence(files):
            manifest = files["manifest.json"]
            manifest["adapter_hashes"] = benchmark.adapter_hashes()
            manifest["fingerprint"] = benchmark.fingerprint(campaign.manifest_identity(manifest))
            files["environments/preserved.json"] = dict(platform=manifest["platform"], architecture="x86_64",
                threads=1, runtime=self.runtime, started_at="2026-10-25T00:00:00Z")
            if alter:
                alter(files)
        return checkpoint(platform=platform, programs=["acousticts", "prol_spheroid", "echosms"],
                          count=6, source_ref="a" * 40, alter=evidence)[0]

    def extend(self, content=None, selected="prol_spheroid", run=123, platform="ubuntu-latest"):
        content = content or self.source(platform=platform)
        api, _ = self.api([dict(id=1, name=f"results-{platform}")], {1: content})
        with patch.dict(os.environ, GITHUB_RUN_ID=str(run + 1)):
            return refresh.resolve_refresh(dict(mode="extend", resume_run_id=str(run), refresh_software=selected),
                                           "owner/repo", "e" * 40, api, sources=self.lock)

    def baseline(self, result, run=124):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            refresh.write_baselines(result, root, "owner/repo", run, sources=self.lock)
            directory = root / result["platforms"][0]
            return {path.relative_to(directory).as_posix(): benchmark.read_json(path)
                    for path in directory.rglob("*.json")}

    def test_extend_promotes_only_selected_measurements_and_keeps_other_software(self):
        content = self.source()
        result = self.extend(content)
        self.assertEqual((result["mode"], result["software"], result["source_ref"]),
                         ("extend", "prol_spheroid", "e" * 40))
        self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 2})
        sidecar = result["_retained_payloads"]["ubuntu-latest"]
        self.assertEqual({row["software"] for row in sidecar["rows"]}, {"acousticts", "echosms"})
        old = {row["case_id"]: row for row in campaign.read_checkpoint(content)[0]["results.json"]}
        self.assertEqual(len(sidecar["reused_records"]), 2)
        for identity, record in sidecar["reused_records"].items():
            expected = {key: value for key, value in old[identity].items()
                        if not key.startswith("reference_") and key != "delta_ts_db"}
            self.assertEqual(record, dict(expected, measurement_origin_run_id=123))
            self.assertEqual(record["software"], "prol_spheroid")
        cases = benchmark.build_cases(result["configuration"], result["profile"], ["prol_spheroid"], 3)
        self.assertEqual(refresh.validate_reused_records(sidecar, cases, self.lock, "Linux", self.runtime),
                         sidecar["reused_records"])

    def test_extend_single_platform_preserves_original_predictions_and_comparisons(self):
        content = self.source()
        entries = [dict(id=1, name="results-ubuntu-latest"), dict(id=2, name="results-windows-latest")]
        api, _ = self.api(entries, {1: content, 2: b"invalid omitted platform"})
        previous_request = api.request.side_effect
        api.request.side_effect = lambda method, path: (dict(jobs=[
            dict(name=f"benchmark ({platform})", steps=None) for platform in campaign.PLATFORMS])
            if "/jobs?" in path else previous_request(method, path))
        result = refresh.resolve_refresh(dict(mode="extend", resume_run_id="123",
            refresh_software="acousticts", continuation_platform="ubuntu-latest"),
            "owner/repo", "e" * 40, api, sources=self.lock)
        self.assertEqual(result["platforms"], ["ubuntu-latest"])
        self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 2})
        self.assertEqual(result["retained_by_platform"], {"ubuntu-latest": 4})
        api.archive.assert_called_once_with(1)
        sidecar = result["_retained_payloads"]["ubuntu-latest"]
        original = {row["case_id"]: row for row in campaign.read_checkpoint(content)[0]["results.json"]}
        for identity, record in sidecar["reused_records"].items():
            expected = {key: value for key, value in original[identity].items()
                        if not key.startswith("reference_") and key != "delta_ts_db"}
            self.assertEqual(record, dict(expected, measurement_origin_run_id=123))
        self.assertEqual({row["software"] for row in sidecar["rows"]}, {"prol_spheroid", "echosms"})

    def test_extend_after_failed_export_keeps_saved_and_unexported_predictions(self):
        def interrupted(files):
            files.pop("status.json")
            row = next(row for row in files["results.json"]
                       if row["software"] == "prol_spheroid" and row["status"] == "pending")
            files[f"records/{row['case_id']}.json"] = dict(case_id=row["case_id"], status="ok",
                elapsed_seconds=.7, ts_db=-41, source_sha="a" * 40, invocation_id="preserved")
        content = self.source(interrupted)
        api, run = self.api([dict(id=1, name="results-ubuntu-latest")], {1: content})
        run["conclusion"] = "failure"
        result = refresh.resolve_refresh(dict(mode="extend", resume_run_id="123", refresh_software="prol_spheroid"),
                                         "owner/repo", "e" * 40, api, sources=self.lock)
        self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 3})
        saved = self.baseline(result)
        sidecar = saved["retained-results.json"]
        self.assertEqual(len(sidecar["rows"]), 4)
        self.assertEqual(len(sidecar["reused_records"]), 3)
        self.assertEqual(sorted(row["elapsed_seconds"] for row in sidecar["reused_records"].values()), [.2, .2, .7])
        self.assertEqual(refresh.baseline_settings(self.archive_files(saved), "owner/repo", 124,
                                                  "ubuntu-latest")["recorded_cases"], 3)

    def test_baseline_binds_seed_records_and_rejects_missing_or_changed_predictions(self):
        result = self.extend()
        files = self.baseline(result)
        settings = refresh.baseline_settings(self.archive_files(files), "owner/repo", 124, "ubuntu-latest")
        self.assertEqual(settings["recorded_cases"], 2)
        record_name = next(name for name in files if name.startswith("records/"))
        for mutation in ("missing", "elapsed", "count"):
            changed = copy.deepcopy(files)
            if mutation == "missing":
                del changed[record_name]
            elif mutation == "elapsed":
                changed[record_name]["elapsed_seconds"] = 999
            else:
                changed["refresh-campaign.json"]["reused_count"] = 0
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, "Reused baseline"):
                refresh.baseline_settings(self.archive_files(changed), "owner/repo", 124, "ubuntu-latest")

    def test_new_baseline_omits_duplicate_reference_plans_without_changing_measurement_evidence(self):
        def reference_plan(files):
            files["manifest.json"]["reference_requests"] = [dict(frequency_hz=1000, details="x" * 10000)]
        result = self.extend(self.source(reference_plan))
        original = copy.deepcopy(result["_retained_payloads"]["ubuntu-latest"])
        saved = self.baseline(result)
        sidecar = saved["retained-results.json"]
        self.assertEqual(sidecar["rows"], original["rows"])
        self.assertEqual(sidecar["reused_records"], original["reused_records"])
        old_proof, new_proof = original["provenance"][0], sidecar["provenance"][0]
        self.assertEqual(new_proof["environments"], old_proof["environments"])
        self.assertEqual(new_proof["archive_sha256"], old_proof["archive_sha256"])
        self.assertEqual(campaign.manifest_identity(new_proof["manifest"]),
                         campaign.manifest_identity(old_proof["manifest"]))
        self.assertEqual(new_proof["manifest"]["fingerprint"], old_proof["manifest"]["fingerprint"])
        self.assertNotIn("reference_requests", new_proof["manifest"])
        self.assertEqual(new_proof["reference_requests_fingerprint"],
                         benchmark.fingerprint(old_proof["manifest"]["reference_requests"]))
        self.assertEqual(refresh.compact_provenance(sidecar), sidecar)
        settings = refresh.baseline_settings(self.archive_files(saved), "owner/repo", 124, "ubuntu-latest")
        self.assertEqual(settings["recorded_cases"], 2)

    def test_refuses_changed_selected_revision_controls_or_adapter(self):
        for mutation in ("source", "controls", "adapter"):
            def alter(files):
                manifest = files["manifest.json"]
                if mutation == "controls":
                    # A selected comparison from another control regime must
                    # never be treated as an already-computed current case.
                    original = manifest["configuration"]["controls"]["prol_spheroid"]
                    original["precision"] = "changed"
                    manifest["fingerprint"] = benchmark.fingerprint(campaign.manifest_identity(manifest))
                elif mutation == "adapter":
                    manifest["adapter_hashes"]["worker_python.py"] = "0" * 64
                    manifest["fingerprint"] = benchmark.fingerprint(campaign.manifest_identity(manifest))
            content = self.source(alter)
            if mutation == "source":
                self.lock["sources"]["prol_spheroid"]["sha"] = "b" * 40
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.extend(content)
            self.lock["sources"]["prol_spheroid"]["sha"] = "a" * 40

    def test_runtime_compares_selected_dependencies_and_compiler_only(self):
        result = self.extend()
        sidecar = result["_retained_payloads"]["ubuntu-latest"]
        cases = benchmark.build_cases(result["configuration"], result["profile"], ["prol_spheroid"], 3)
        compatible = copy.deepcopy(self.runtime)
        compatible["environment_compatibility"].pop("r_packages")
        compatible["environment_compatibility"].pop("julia_environments")
        compatible["environment_compatibility"]["python_packages"] += "unrelated-package==1.0\n"
        refresh.validate_reused_records(sidecar, cases, self.lock, "Linux", compatible)
        compatible["environment_compatibility"]["prol_compiler"] = "different compiler"
        with self.assertRaisesRegex(ValueError, "compiler or dependencies"):
            refresh.validate_reused_records(sidecar, cases, self.lock, "Linux", compatible)

    def test_cross_platform_adapter_comparison_accepts_only_line_ending_differences(self):
        def windows(files):
            hashes = files["manifest.json"]["adapter_hashes"]
            for name in refresh._measurement_adapters("prol_spheroid"):
                raw = (ROOT / "src" / name).read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n")
                hashes[name] = refresh.hashlib.sha256(raw).hexdigest()
            files["manifest.json"]["fingerprint"] = benchmark.fingerprint(campaign.manifest_identity(files["manifest.json"]))
        result = self.extend(self.source(windows, "windows-latest"), platform="windows-latest")
        self.assertEqual(result["recorded_by_platform"], {"windows-latest": 2})

    def test_chain_prol_to_echosms_to_liquid_keeps_seeded_and_other_comparisons(self):
        prol = self.extend()
        prol_files = self.baseline(prol, 124)
        echo = self.extend(self.archive_files(prol_files), "echosms", 124)
        self.assertEqual(echo["recorded_by_platform"], {"ubuntu-latest": 2})
        echo_sidecar = echo["_retained_payloads"]["ubuntu-latest"]
        self.assertEqual({row["software"] for row in echo_sidecar["rows"]}, {"acousticts", "prol_spheroid"})
        self.assertEqual({row["measurement_origin_run_id"] for row in echo_sidecar["rows"]
                          if row["software"] == "prol_spheroid"}, {123})
        echo_files = self.baseline(echo, 125)
        liquid = self.extend(self.archive_files(echo_files), "liquid_spheroid", 125)
        self.assertEqual(liquid["recorded_by_platform"], {"ubuntu-latest": 0})
        saved = liquid["_retained_payloads"]["ubuntu-latest"]["rows"]
        self.assertEqual(len(saved), 6)
        self.assertEqual({row["software"] for row in saved}, {"acousticts", "prol_spheroid", "echosms"})
        self.assertTrue(all(row["elapsed_seconds"] == .2 and row["source_sha"] == "a" * 40 for row in saved))

    def test_extending_preserves_prior_failures_and_drops_derived_reference_fields(self):
        def failed(files):
            identity = next(row["case_id"] for row in files["results.json"]
                            if row["software"] == "prol_spheroid" and row["status"] == "ok")
            files[f"records/{identity}.json"].update(status="nonconverged", elapsed_seconds=2.)
            row = next(row for row in files["results.json"] if row["case_id"] == identity)
            row.update(status="nonconverged", elapsed_seconds=2., reference_ts_db=-41,
                       reference_id="b" * 64, delta_ts_db=None)
        result = self.extend(self.source(failed))
        seeds = result["_retained_payloads"]["ubuntu-latest"]["reused_records"]
        self.assertEqual(sum(record["status"] == "nonconverged" for record in seeds.values()), 1)
        self.assertTrue(all(not key.startswith("reference_") and key != "delta_ts_db"
                            for record in seeds.values() for key in record))


class ContinuationFrequencyTests(unittest.TestCase):
    api = CampaignTests.api
    archive_files = staticmethod(RefreshTests.archive_files)
    setUp = ExtendTests.setUp

    def completed_campaign(self, configuration, programs, retained=None, run=123):
        def populate(files):
            manifest = files["manifest.json"]
            manifest["configuration"] = copy.deepcopy(configuration)
            cases = benchmark.build_cases(configuration, "pilot", programs, 1)
            manifest["cases"] = [case["case_id"] for case in cases]
            manifest["adapter_hashes"] = benchmark.adapter_hashes()
            seeds = retained.get("reused_records", {}) if retained else {}
            records = {case["case_id"]: copy.deepcopy(seeds.get(case["case_id"],
                dict(case_id=case["case_id"], status="ok", elapsed_seconds=.2 + case["frequency_hz"] / 1e6,
                     ts_db=-40 + case["frequency_hz"] / 1e6, source_sha="a" * 40,
                     invocation_id="preserved"))) for case in cases}
            if retained:
                files["retained-results.json"] = copy.deepcopy(retained)
                manifest["retained_fingerprint"] = benchmark.fingerprint(retained)
            manifest["fingerprint"] = benchmark.fingerprint(campaign.manifest_identity(manifest))
            files["results.json"] = benchmark.summarize(cases, records, {})
            files["status.json"] = dict(state="complete", recorded=len(cases), planned=len(cases))
            files.update({f"records/{identity}.json": record for identity, record in records.items()})
            files["environments/preserved.json"] = dict(platform="Linux", architecture="x86_64",
                threads=1, runtime=self.runtime, started_at="2026-10-28T00:00:00Z")
            files["benchmark-source.json"].update(run_id=run, resumed_from_run_id=None)
        return checkpoint(programs=programs, repeats=1, count=0, source_ref="a" * 40,
                          alter=populate)[0]

    def original_campaign(self, programs=None, stop_hz=300000):
        programs = programs or ["acousticts", "prol_spheroid", "echosms"]
        config = configuration()
        config.update(software=programs, repetitions=1, case_order="boundary_major",
            frequency=dict(start_hz=2000, stop_hz=stop_hz, step_hz=2000, include_stop=True))
        return self.completed_campaign(config, programs)

    def resolve(self, content, inputs=None, run=123):
        api, _ = self.api([dict(id=1, name="results-ubuntu-latest")], {1: content})
        selection = dict(mode="extend", resume_run_id=str(run), refresh_software="liquid_spheroid",
            continuation_frequency="Use frequency inputs above", frequency_start_khz="2",
            frequency_end_khz="100", frequency_interval_khz="2")
        selection.update(inputs or {})
        with patch.dict(os.environ, GITHUB_RUN_ID=str(run + 1)):
            return refresh.resolve_refresh(selection, "owner/repo", "e" * 40, api, sources=self.lock)

    def test_selected_software_frequency_grid_preserves_all_comparison_measurements(self):
        content = self.original_campaign()
        original = campaign.read_checkpoint(content)[0]["results.json"]
        for mode in ("extend", "refresh"):
            with self.subTest(mode=mode):
                result = self.resolve(content, dict(mode=mode))
                cases = benchmark.build_cases(result["configuration"], result["profile"],
                                              ["liquid_spheroid"], result["repetitions"])
                self.assertEqual([case["frequency_hz"] for case in cases], list(range(2000, 100001, 2000)))
                self.assertEqual(result["planned_cases"], 50)
                self.assertEqual(result["software"], "liquid_spheroid")
                self.assertEqual(result["recorded_by_platform"], {"ubuntu-latest": 0})
                retained = result["_retained_payloads"]["ubuntu-latest"]["rows"]
                self.assertEqual(retained, [dict(row, measurement_role="retained", retained_from_run_id=123)
                                            for row in original])
                self.assertEqual({row["software"] for row in retained},
                                 {"acousticts", "prol_spheroid", "echosms"})
                self.assertTrue(any(row["frequency_hz"] > 100000 for row in retained))
                self.assertEqual(result["configuration"]["geometry"], configuration()["geometry"])
                self.assertEqual(result["configuration"]["controls"], configuration()["controls"])

    def test_default_continuation_frequency_keeps_saved_grid(self):
        content = self.original_campaign()
        for mode in ("extend", "refresh"):
            for explicit in (False, True):
                with self.subTest(mode=mode, explicit=explicit):
                    inputs = dict(mode=mode, resume_run_id="123", refresh_software="liquid_spheroid",
                        frequency_start_khz="2", frequency_end_khz="100", frequency_interval_khz="2")
                    if explicit:
                        inputs["continuation_frequency"] = "Keep saved frequency grid"
                    api, _ = self.api([dict(id=1, name="results-ubuntu-latest")], {1: content})
                    result = refresh.resolve_refresh(inputs, "owner/repo", "e" * 40, api, sources=self.lock)
                    self.assertEqual(result["configuration"]["frequency"],
                        dict(start_hz=2000, stop_hz=300000, step_hz=2000, include_stop=True))
                    self.assertEqual(result["planned_cases"], 150)

    def test_nested_frequency_expansions_reuse_original_measurements(self):
        result = self.resolve(self.original_campaign())
        comparisons = copy.deepcopy(result["_retained_payloads"]["ubuntu-latest"]["rows"])
        origin_records = {}
        for run, stop_khz in ((124, 200), (125, 300)):
            content = self.completed_campaign(result["configuration"], ["liquid_spheroid"],
                result["_retained_payloads"]["ubuntu-latest"], run=run)
            saved = campaign.read_checkpoint(content)[0]["results.json"]
            if not origin_records:
                origin_records = {row["case_id"]: row for row in saved}
            result = self.resolve(content, dict(frequency_end_khz=str(stop_khz)), run=run)
            sidecar = result["_retained_payloads"]["ubuntu-latest"]
            seeds = sidecar["reused_records"]
            self.assertEqual(len(seeds), (stop_khz - 100) // 2)
            self.assertEqual(result["planned_cases"], stop_khz // 2)
            self.assertEqual(sidecar["rows"], comparisons)
            for row in saved:
                expected = {key: value for key, value in row.items()
                            if not key.startswith("reference_") and key != "delta_ts_db"}
                expected.setdefault("measurement_origin_run_id", run)
                self.assertEqual(seeds[row["case_id"]], expected)
            for identity, original in origin_records.items():
                self.assertEqual(seeds[identity]["elapsed_seconds"], original["elapsed_seconds"])
                self.assertEqual(seeds[identity]["ts_db"], original["ts_db"])
                self.assertEqual(seeds[identity]["measurement_origin_run_id"], 124)

    def test_continuation_frequency_rejects_omitted_selected_measurements(self):
        content = self.original_campaign(["liquid_spheroid"], stop_hz=100000)
        with self.assertRaisesRegex(ValueError, "include every saved selected-software case"):
            self.resolve(content, dict(frequency_end_khz="98"))

    def test_invalid_continuation_grid_is_rejected_before_case_allocation(self):
        content = self.original_campaign()
        invalid = [dict(frequency_start_khz="0"), dict(frequency_interval_khz="0"),
                   dict(frequency_interval_khz="0.0001"), dict(frequency_end_khz="nan"),
                   dict(frequency_start_khz="101"),
                   dict(frequency_end_khz="1000000000", frequency_interval_khz="0.001")]
        build_cases = refresh.build_cases
        for inputs in invalid:
            with self.subTest(inputs=inputs), patch.object(refresh, "build_cases", wraps=build_cases) as allocate:
                with self.assertRaises(ValueError):
                    self.resolve(content, inputs)
                for call in allocate.call_args_list:
                    self.assertEqual(call.args[0]["frequency"],
                        dict(start_hz=2000, stop_hz=300000, step_hz=2000, include_stop=True))

    def test_continuation_frequency_rejects_unsupported_modes_and_unknown_choice(self):
        for mode in ("new", "resume", "prepare"):
            api = Mock()
            with self.subTest(mode=mode), self.assertRaisesRegex(ValueError, "require extend or refresh"):
                campaign.resolve(dict(mode=mode, continuation_frequency="Use frequency inputs above"),
                                 configuration(), "owner/repo", "e" * 40, api)
            api.request.assert_not_called()
        for mode in ("extend", "refresh"):
            api = Mock()
            with self.subTest(mode=mode), self.assertRaisesRegex(ValueError, "Unknown continuation_frequency"):
                refresh.resolve_refresh(dict(mode=mode, resume_run_id="123", continuation_frequency="unknown"),
                                        "owner/repo", "e" * 40, api, sources=self.lock)
            api.request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
