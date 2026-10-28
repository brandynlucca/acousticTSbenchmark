"""Export saved manuscript BEM/FEM-BEM references without running numerical solvers.

The local spatial criterion remains distinct from the benchmark's independently
qualified reference protocol. Input plotting validators are loaded only by the
explicit local export command and their file hashes accompany the exported data.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import io
import json
import math
from pathlib import Path
import re
import tomllib

from benchmark import PHYSICAL_FIELDS, fingerprint, physical_identity, utc_now

BOUNDARIES = {"fixed_rigid": "rigid", "pressure_release": "pressure_release",
              "liquid_filled": "liquid", "gas_filled": "gas"}
SHAPES = ("ar0101_a0001", "ar0101_a0010", "ar0101_a0500", "ar1000_a0001",
          "ar1000_a0010", "ar1000_a0500", "ar5000_a0001", "ar5000_a0010",
          "ar5000_a0500", "ar10000_a0070")
QUALIFICATION = "local spatial convergence"


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def read_csv(path):
    raw = Path(path).read_bytes()
    return list(csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))), digest(raw)


def read_checkpoint(path):
    """Hash the entire raw record and parse only its header and canonical row."""
    hasher = hashlib.sha256()
    header, row = [], []
    in_header, in_row = True, False
    with Path(path).open("rb") as stream:
        for line in stream:
            hasher.update(line)
            text = line.decode("utf-8")
            if text.strip() == "[row]":
                if in_row:
                    raise ValueError(f"Duplicate canonical row: {path}")
                in_row = True
            if text.lstrip().startswith("["):
                in_header = False
            if in_header:
                header.append(text)
            if in_row:
                row.append(text)
    if not row:
        raise ValueError(f"Missing canonical row: {path}")
    return dict(configuration_sha256=tomllib.loads("".join(header))["configuration_sha256"],
                row=tomllib.loads("".join(row))["row"], raw_sha256=hasher.hexdigest())


def finite(value, name):
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"Nonfinite {name}")
    return value


def physical_case(config, boundary, row):
    boundary = BOUNDARIES[boundary]
    body = config["body"]
    interior = config.get("interior", {})
    fluid = boundary in ("liquid", "gas")
    density = interior.get("density_kg_m3", config.get("density_interior_kg_m3"))
    speed = interior.get("soundspeed_m_s", config.get("soundspeed_interior_m_s"))
    result = dict(boundary=boundary, frequency_hz=finite(row["frequency_Hz"], "frequency"),
        incidence_deg=finite(row["angle_deg"], "incidence"),
        semimajor_m=finite(body["a_m"], "semimajor axis"),
        semiminor_m=finite(body["b_m"], "semiminor axis"),
        medium_density_kg_m3=finite(config["density_exterior_kg_m3"], "medium density"),
        medium_sound_speed_m_s=finite(config["soundspeed_exterior_m_s"], "medium speed"),
        target_density_kg_m3=finite(density, "target density") if fluid else None,
        target_sound_speed_m_s=finite(speed, "target speed") if fluid else None)
    if (any(result[key] <= 0 for key in PHYSICAL_FIELDS if key not in
            ("boundary", "incidence_deg") and result[key] is not None)
            or result["semiminor_m"] > result["semimajor_m"]
            or not 0 <= result["incidence_deg"] <= 180):
        raise ValueError("Invalid physical inputs")
    return result


def validated_reference(csv_row, saved, config, accepted, boundary, raw_path,
                        protocol_sha256, csv_sha256, source_csv):
    """Require exact saved-row agreement and the original spatial criterion."""
    stored = saved["row"]
    configuration = saved["configuration_sha256"]
    if configuration not in accepted:
        raise ValueError("Unaccepted numerical configuration")
    if csv_row.get("configuration_sha256", configuration) != configuration:
        raise ValueError("CSV configuration differs from raw checkpoint")
    if (stored.get("status") != "spatial_converged"
            or stored.get("spatial_converged") is not True
            or csv_row.get("status", "spatial_converged") != "spatial_converged"
            or str(csv_row.get("spatial_converged", "true")).lower() != "true"):
        raise ValueError("Reference is not spatially converged")
    if config.get("reference_data_used") is not False:
        raise ValueError("Numerical reference used comparison output")
    for field in ("frequency_Hz", "angle_deg", "mesh_change_db"):
        if finite(csv_row[field], field) != finite(stored[field], field):
            raise ValueError(f"CSV/raw mismatch: {field}")
    ts = finite(csv_row.get("numerical_TS_dB", csv_row.get("TS_dB")), "TS")
    if ts != finite(stored["TS_dB"], "raw TS"):
        raise ValueError("CSV/raw mismatch: TS_dB")
    for field in ("seconds", "amplitude_real_m", "amplitude_imag_m"):
        if field in csv_row and finite(csv_row[field], field) != finite(stored[field], field):
            raise ValueError(f"CSV/raw mismatch: {field}")
    path = Path(raw_path).resolve()
    if Path(csv_row["raw_record"]).resolve() != path or Path(stored["raw_record"]).resolve() != path:
        raise ValueError("Unexpected raw checkpoint path")
    if (finite(stored["frequency_Hz"], "frequency") not in config["frequencies_Hz"]
            or finite(stored["angle_deg"], "angle") not in config["angles_deg"]):
        raise ValueError("Unplanned reference input")
    tolerance = finite(config["controls"]["target_tol"], "spatial tolerance")
    change = finite(stored["mesh_change_db"], "spatial change")
    if not 0 <= change <= tolerance or not 0 < tolerance <= .005:
        raise ValueError("Reference exceeds the accepted spatial tolerance")
    case = physical_case(config, boundary, stored)
    a, b = case["semimajor_m"], case["semiminor_m"]
    angular_frequency = 2 * math.pi * case["frequency_hz"]
    focal = math.sqrt((a - b) * (a + b))
    shape_match = re.fullmatch(r"ar(\d+)_a\d+", config.get("shape_code", ""))
    aspect = int(shape_match.group(1)) / 100 if shape_match else config.get("aspect_ratio", a / b)
    if not math.isclose(aspect, a / b, rel_tol=1e-12):
        raise ValueError("Shape aspect ratio disagrees with physical axes")
    if math.isclose(aspect, round(aspect), rel_tol=0, abs_tol=1e-12):
        aspect = float(round(aspect))
    result = dict(case, physical_id=physical_identity(case), aspect_ratio=aspect,
        k0a=angular_frequency * a / case["medium_sound_speed_m_s"],
        h0=angular_frequency * focal / case["medium_sound_speed_m_s"],
        hs=angular_frequency * focal / case["target_sound_speed_m_s"] if case["target_sound_speed_m_s"] else None,
        ts_db=ts, method="fem_bem" if case["boundary"] in ("gas", "liquid") else "bem",
        origin="local_manuscript", qualification=QUALIFICATION, uncertainty_db=None,
        mesh_change_db=change, spatial_tolerance_db=tolerance,
        configuration_sha256=configuration, protocol_sha256=protocol_sha256,
        raw_sha256=saved["raw_sha256"], raw_record=str(path), source_csv=str(source_csv),
        source_csv_sha256=csv_sha256, provenance_id=configuration, source_sha=None,
        independent_qualification=stored.get("independent_qualification", "not_established_for_this_case"))
    real, imag = stored.get("amplitude_real_m"), stored.get("amplitude_imag_m")
    if real is not None and imag is not None:
        real, imag = finite(real, "real amplitude"), finite(imag, "imaginary amplitude")
        magnitude = math.hypot(real, imag)
        if magnitude <= 0 or abs(20 * math.log10(magnitude) - ts) > 1e-8:
            raise ValueError("Physical amplitude does not reconstruct TS")
        result.update(amplitude_real_m=real, amplitude_imag_m=imag)
    result["reference_id"] = fingerprint(dict(physical_id=result["physical_id"],
        configuration_sha256=configuration, raw_sha256=saved["raw_sha256"]))
    return result


def add_reference(records, reference):
    key = reference["physical_id"]
    if key in records and records[key]["reference_id"] != reference["reference_id"]:
        raise ValueError(f"Conflicting local references for physical input {key}")
    records[key] = reference


def load_validator(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def export_references(manuscript, numerical_root):
    manuscript, numerical_root = Path(manuscript).resolve(), Path(numerical_root).resolve()
    shape_script = manuscript / "scripts/plot_psms_paper_spectra.py"
    base_script = manuscript / "scripts/plot_psms_paper_fem_frequency_spectra.py"
    shape_validator = load_validator(shape_script, "local_shape_reference_validator")
    reader = load_validator(base_script, "local_fem_reference_validator")
    reader.FEM_CSV = numerical_root / "liquid-2k-1mhz-step2k-results.csv"
    sources = {str(p): digest(p.read_bytes()) for p in (shape_script, base_script)}
    protocols, protocol_index, records, counts = {}, {}, {}, {}

    def protocol(path):
        path = Path(path).resolve()
        raw = path.read_bytes()
        value = tomllib.loads(raw.decode("utf-8"))
        identity = value["configuration_sha256"]
        sources[str(path)] = digest(raw)
        if identity in protocol_index and protocol_index[identity]["configuration"] != value["configuration"]:
            raise ValueError("Conflicting numerical configuration identity")
        protocol_index[identity] = value
        protocols.setdefault(identity, dict(protocol_path=str(path), protocol_sha256=digest(raw),
            source_sha=None, source_identity_kind="recorded file hashes, clean Git revision not established",
            source_file_sha256=value["configuration"]["source_sha256"],
            configuration=value["configuration"]))
        return value

    def consume(csv_path, rows, csv_hash, current_config, accepted, prefix, boundary):
        count = 0
        for csv_row in rows:
            if csv_row.get("status", "spatial_converged") != "spatial_converged":
                continue
            frequency = finite(csv_row["frequency_Hz"], "frequency")
            angle = finite(csv_row["angle_deg"], "angle")
            if not frequency.is_integer() or not angle.is_integer():
                raise ValueError("Nonintegral manuscript spectrum input")
            raw_path = Path(f"{prefix}-{int(frequency):07d}Hz-{int(angle):03d}deg.toml")
            if raw_path.resolve().parent != numerical_root:
                raise ValueError("Raw checkpoint outside the numerical source directory")
            saved = read_checkpoint(raw_path)
            config_id = saved["configuration_sha256"]
            if config_id not in accepted or config_id not in protocol_index:
                raise ValueError("Raw checkpoint lacks an accepted saved protocol")
            config = protocol_index[config_id]["configuration"]
            # The accepted historical protocol supplies its original solver controls.
            for field in ("body", "density_exterior_kg_m3", "soundspeed_exterior_m_s"):
                if config[field] != current_config[field]:
                    raise ValueError("Inherited reference changed physical inputs")
            reference = validated_reference(csv_row, saved, config, accepted, boundary, raw_path,
                protocols[config_id]["protocol_sha256"], csv_hash, csv_path)
            add_reference(records, reference)
            count += 1
        return count

    batch_path = next((numerical_root / (name + "-protocol.toml") for name in
        ("manuscript-coarse-coverage-accelerated", "manuscript-coarse-coverage")
        if (numerical_root / (name + "-protocol.toml")).exists()), None)
    if batch_path is None:
        raise ValueError("Missing accepted manuscript numerical execution plan")
    batch_raw = batch_path.read_bytes()
    batch = tomllib.loads(batch_raw.decode("utf-8"))
    sources[str(batch_path)] = digest(batch_raw)
    jobs = {(job["shape"], job["name"]): job for job in batch["configuration"]["jobs"]}
    for shape in SHAPES:
        csv_path = manuscript / "data" / f"psms_paper_shape_fem_{shape}.csv"
        rows, csv_hash = read_csv(csv_path)
        sources[str(csv_path)] = csv_hash
        counts[csv_path.name] = 0
        for boundary in BOUNDARIES:
            selected = [r for r in rows if r["boundary"] == boundary]
            if not selected:
                continue
            if any(r["shape"] != shape for r in selected):
                raise ValueError("Exported shape differs from its source CSV")
            job = jobs[(shape, boundary)]
            prefix = job["prefix"]
            suffix = job.get("execution_suffix", "-coarse-execution")
            current = protocol(prefix + suffix + "-protocol.toml")
            if current["configuration"] != job["configuration"]:
                raise ValueError("Numerical protocol differs from the accepted batch plan")
            ancestors = []
            accepted = shape_validator.shape_protocol_chain(current, prefix, reader, sources, ancestors)
            for path in Path(prefix).parent.glob(Path(prefix).name + "*-protocol.toml"):
                candidate = tomllib.loads(path.read_text(encoding="utf-8"))
                if candidate.get("configuration_sha256") in accepted:
                    protocol(path)
            counts[csv_path.name] += consume(csv_path, selected, csv_hash,
                current["configuration"], accepted, prefix, boundary)

    queue_path = numerical_root / "boundary-2k-400k-step2k-queue-execution-protocol.toml"
    queue_raw = queue_path.read_bytes()
    queue = tomllib.loads(queue_raw.decode("utf-8"))
    sources[str(queue_path)] = digest(queue_raw)
    base_jobs = {j["name"]: j["configuration"] for j in queue["configuration"]["jobs"]}
    for boundary in BOUNDARIES:
        name = "liquid-2k-1mhz-step2k" if boundary == "liquid_filled" else boundary.replace("_", "-") + "-2k-400k-step2k"
        prefix = str(numerical_root / name)
        current = protocol(prefix + "-protocol.toml")
        if boundary == "liquid_filled":
            accepted = reader.accepted_fem_protocols()
            for suffix in ("-continuation-protocol.toml", "-accelerated-execution-protocol.toml"):
                path = Path(prefix + suffix)
                if path.exists():
                    protocol(path)
            extra = numerical_root / "boundary-2k-400k-step2k-queue-serialization-continuation-protocol.toml"
            if extra.exists():
                sources[str(extra)] = digest(extra.read_bytes())
        else:
            if current["configuration"] != base_jobs[boundary]:
                raise ValueError("Base boundary protocol differs from accepted execution plan")
            accepted = {current["configuration_sha256"]}
        csv_path = reader.active_fem_csv(Path(prefix + "-results.csv"))
        rows, csv_hash = read_csv(csv_path)
        sources[str(csv_path)] = csv_hash
        counts[csv_path.name] = consume(csv_path, rows, csv_hash,
            current["configuration"], accepted, prefix, boundary)
    ordered = sorted(records.values(), key=lambda r: (r["semimajor_m"], r["aspect_ratio"],
        r["boundary"], r["incidence_deg"], r["frequency_hz"]))
    return dict(schema_version=1, exported_at=utc_now(), origin="local_manuscript",
        qualification=QUALIFICATION, numerical_solves_run=0, reference_curves=ordered,
        protocols=protocols, source_file_sha256=sources, source_counts=counts,
        reference_count=len(ordered), content_sha256=fingerprint(ordered))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manuscript", type=Path, required=True)
    parser.add_argument("--numerical-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.output.parent.is_dir():
        raise ValueError("Output directory must already exist")
    result = export_references(args.manuscript, args.numerical_root)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(json.dumps(result, separators=(",", ":"), allow_nan=False) + "\n", encoding="utf-8")
    temporary.replace(args.output)
    print(json.dumps(dict(output=str(args.output), reference_count=result["reference_count"],
        content_sha256=result["content_sha256"], source_counts=result["source_counts"])))


if __name__ == "__main__":
    main()
