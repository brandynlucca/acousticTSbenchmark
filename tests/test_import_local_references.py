"""Self-contained checks for importing saved local spatial references."""
import copy
import math
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import benchmark
import import_local_references as local


def example(boundary="gas_filled"):
    config = dict(body=dict(a_m=.01, b_m=.001), aspect_ratio=10.,
        density_exterior_kg_m3=1026.8, soundspeed_exterior_m_s=1477.3,
        interior=dict(density_kg_m3=1.24, soundspeed_m_s=345.),
        frequencies_Hz=[2000], angles_deg=[37], controls=dict(target_tol=.005),
        reference_data_used=False)
    raw_path = Path("saved-case.toml").resolve()
    row = dict(frequency_Hz=2000, angle_deg=37, status="spatial_converged",
        spatial_converged=True, TS_dB=-40., mesh_change_db=.002, seconds=3.,
        amplitude_real_m=.006, amplitude_imag_m=.008, raw_record=str(raw_path),
        independent_qualification="not_established_for_this_case")
    csv_row = {key: str(value) for key, value in row.items()}
    csv_row.update(numerical_TS_dB=csv_row.pop("TS_dB"), configuration_sha256="a" * 64)
    saved = dict(row=row, configuration_sha256="a" * 64, raw_sha256="b" * 64)
    args = dict(csv_row=csv_row, saved=saved, config=config, accepted={"a" * 64},
        boundary=boundary, raw_path=raw_path, protocol_sha256="c" * 64,
        csv_sha256="d" * 64, source_csv="source.csv")
    return args


class LocalReferenceImportTests(unittest.TestCase):
    def test_preserves_physics_amplitude_and_local_qualification(self):
        result = local.validated_reference(**example())
        self.assertEqual(result["physical_id"], benchmark.physical_identity(result))
        self.assertEqual(result["method"], "fem_bem")
        self.assertEqual(result["qualification"], "local spatial convergence")
        self.assertEqual(result["independent_qualification"], "not_established_for_this_case")
        self.assertIsNone(result["uncertainty_db"])
        self.assertIsNone(result["source_sha"])
        self.assertEqual(result["amplitude_real_m"], .006)
        self.assertAlmostEqual(result["h0"], 2 * math.pi * 2000 * math.sqrt((.01-.001)*(.01+.001))/1477.3)
        self.assertEqual(result["raw_sha256"], "b" * 64)
        self.assertNotIn("elapsed_seconds", result)
        self.assertNotIn("memory_delta_peak_bytes", result)

    def test_hard_boundary_has_no_material_ratios(self):
        result = local.validated_reference(**example("fixed_rigid"))
        self.assertEqual(result["boundary"], "rigid")
        self.assertEqual(result["method"], "bem")
        self.assertIsNone(result["target_density_kg_m3"])
        self.assertIsNone(result["target_sound_speed_m_s"])
        self.assertIsNone(result["hs"])

    def test_original_liquid_protocol_material_fields(self):
        args = example("liquid_filled")
        args["config"].pop("interior")
        args["config"].update(density_interior_kg_m3=1028.9, soundspeed_interior_m_s=1480.3)
        result = local.validated_reference(**args)
        self.assertEqual(result["target_density_kg_m3"], 1028.9)
        self.assertEqual(result["target_sound_speed_m_s"], 1480.3)

    def test_aspect_display_keeps_intended_ratio_and_exact_physical_axes(self):
        args = example()
        args["config"].update(body=dict(a_m=.07, b_m=.0007000000000000001),
                              aspect_ratio=100.00000000000001, shape_code="ar10000_a0070")
        result = local.validated_reference(**args)
        self.assertEqual(result["aspect_ratio"], 100.)
        self.assertEqual(result["semiminor_m"], .0007000000000000001)
        self.assertEqual(result["physical_id"], benchmark.physical_identity(result))
        args["config"].update(body=dict(a_m=.07, b_m=.01))
        args["config"].pop("aspect_ratio")
        args["config"].pop("shape_code")
        self.assertEqual(local.validated_reference(**args)["aspect_ratio"], 7.)

    def test_rejects_mismatched_csv_or_unaccepted_protocol(self):
        for field, value in (("numerical_TS_dB", "-41"), ("frequency_Hz", "4000"),
                             ("mesh_change_db", ".003"), ("configuration_sha256", "f" * 64),
                             ("raw_record", "another.toml")):
            with self.subTest(field=field):
                args = example()
                args["csv_row"][field] = value
                with self.assertRaises(ValueError):
                    local.validated_reference(**args)
        args = example()
        args["accepted"] = {"f" * 64}
        with self.assertRaisesRegex(ValueError, "Unaccepted"):
            local.validated_reference(**args)

    def test_rejects_nonconvergence_nonfinite_and_excess_spatial_change(self):
        for field, value in (("status", "unresolved"), ("spatial_converged", False),
                             ("mesh_change_db", .006), ("TS_dB", math.nan)):
            with self.subTest(field=field):
                args = example()
                args["saved"]["row"][field] = value
                args["csv_row"]["numerical_TS_dB" if field == "TS_dB" else field] = str(value)
                with self.assertRaises(ValueError):
                    local.validated_reference(**args)

    def test_rejects_changed_amplitude_or_unplanned_input(self):
        args = example()
        args["saved"]["row"]["amplitude_real_m"] = .009
        args["csv_row"]["amplitude_real_m"] = ".009"
        with self.assertRaisesRegex(ValueError, "reconstruct"):
            local.validated_reference(**args)
        args = example()
        args["config"]["frequencies_Hz"] = [4000]
        with self.assertRaisesRegex(ValueError, "Unplanned"):
            local.validated_reference(**args)

    def test_duplicate_identity_is_idempotent_but_conflict_is_rejected(self):
        result = local.validated_reference(**example())
        records = {}
        local.add_reference(records, result)
        local.add_reference(records, copy.deepcopy(result))
        self.assertEqual(len(records), 1)
        changed = dict(result, reference_id="other")
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            local.add_reference(records, changed)

    def test_reads_only_header_and_row_but_hashes_all_raw_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "record.toml"
            raw = ('configuration_sha256 = "abc"\n[data]\nvalue = +inf\n'
                   '[row]\nTS_dB = -40.0\nspatial_converged = true\n').encode()
            path.write_bytes(raw)
            result = local.read_checkpoint(path)
            self.assertEqual(result["configuration_sha256"], "abc")
            self.assertEqual(result["row"]["TS_dB"], -40.)
            self.assertEqual(result["raw_sha256"], local.digest(raw))
            self.assertNotIn("data", result)


if __name__ == "__main__":
    unittest.main()
