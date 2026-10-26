"""Incrementally collate immutable benchmark observations in a SQLite database."""

from __future__ import annotations

from contextlib import contextmanager
import csv
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import re
import sqlite3
import zipfile
import zlib

from benchmark import CAPABILITIES, PHYSICAL_FIELDS, STATUSES, fingerprint, physical_identity


SCHEMA_VERSION = 1
CASE_FIELDS = (*PHYSICAL_FIELDS, "software", "repetition", "aspect_ratio", "k0a", "h0", "hs",
               "controls", "warmup", "physical_id")
MANIFEST_FIELDS = ("configuration", "sources", "profile", "cases", "adapter_hashes", "platform",
                   "architecture", "environment_fingerprint", "workload", "retained_fingerprint")
PLATFORMS = {"Linux": "ubuntu-latest", "Windows": "windows-latest", "Darwin": "macos-latest"}
MAX_ENTRY_BYTES = 1024 * 1024 * 1024
MAX_ARCHIVE_ENTRIES = 1_001_000

SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS artifacts (
 artifact_id INTEGER PRIMARY KEY, repository TEXT NOT NULL, run_id INTEGER,
 name TEXT NOT NULL, created_at TEXT, archive_sha256 TEXT NOT NULL,
 imported_at TEXT NOT NULL, metadata_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS cases (
 case_id TEXT PRIMARY KEY, physical_id TEXT NOT NULL, software TEXT NOT NULL,
 boundary TEXT NOT NULL, frequency_hz REAL NOT NULL, incidence_deg REAL NOT NULL,
 semimajor_m REAL NOT NULL, semiminor_m REAL NOT NULL, aspect_ratio REAL NOT NULL,
 medium_density_kg_m3 REAL NOT NULL, medium_sound_speed_m_s REAL NOT NULL,
 target_density_kg_m3 REAL, target_sound_speed_m_s REAL,
 repetition INTEGER NOT NULL, k0a REAL, h0 REAL, hs REAL, warmup INTEGER,
 controls_json TEXT NOT NULL, case_json TEXT NOT NULL, applicable INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS cases_selection ON cases
 (software, semimajor_m, aspect_ratio, boundary, incidence_deg, frequency_hz);
CREATE INDEX IF NOT EXISTS cases_physical ON cases (physical_id);
CREATE TABLE IF NOT EXISTS manifests (
 manifest_fingerprint TEXT PRIMARY KEY, platform TEXT, source_json TEXT NOT NULL,
 manifest_zlib BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS artifact_manifests (
 artifact_id INTEGER NOT NULL REFERENCES artifacts, manifest_fingerprint TEXT NOT NULL REFERENCES manifests,
 PRIMARY KEY (artifact_id, manifest_fingerprint));
CREATE TABLE IF NOT EXISTS environments (
 environment_fingerprint TEXT PRIMARY KEY, evidence_zlib BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS invocations (
 platform TEXT NOT NULL, invocation_id TEXT NOT NULL,
 environment_fingerprint TEXT NOT NULL REFERENCES environments,
 origin_run_id INTEGER, origin_run_attempt INTEGER, started_at TEXT,
 PRIMARY KEY (platform, invocation_id));
CREATE TABLE IF NOT EXISTS observations (
 observation_id TEXT PRIMARY KEY, case_id TEXT NOT NULL REFERENCES cases,
 platform TEXT NOT NULL, source_sha TEXT NOT NULL, invocation_id TEXT,
 origin_run_id INTEGER, origin_run_attempt INTEGER, finished_at TEXT,
 status TEXT NOT NULL, elapsed_seconds REAL, wall_seconds REAL, warmup_seconds REAL,
 ts_db REAL, amplitude_real_m REAL, amplitude_imag_m REAL,
 memory_delta_peak_bytes INTEGER, memory_baseline_rss_bytes INTEGER, memory_peak_rss_bytes INTEGER,
 memory_method TEXT, memory_status TEXT, memory_samples INTEGER,
 memory_sample_interval_seconds REAL, record_sha256 TEXT NOT NULL, record_zlib BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS observations_case ON observations (platform, case_id, finished_at);
CREATE INDEX IF NOT EXISTS observations_version ON observations (source_sha, platform);
CREATE TABLE IF NOT EXISTS observation_artifacts (
 observation_id TEXT NOT NULL REFERENCES observations, artifact_id INTEGER NOT NULL REFERENCES artifacts,
 PRIMARY KEY (observation_id, artifact_id));
CREATE TABLE IF NOT EXISTS planned_cases (
 artifact_id INTEGER NOT NULL REFERENCES artifacts, case_id TEXT NOT NULL REFERENCES cases,
 platform TEXT NOT NULL, source_sha TEXT,
 PRIMARY KEY (artifact_id, case_id, platform));
CREATE TABLE IF NOT EXISTS reference_results (
 reference_fingerprint TEXT PRIMARY KEY, reference_id TEXT NOT NULL, physical_id TEXT NOT NULL,
 boundary TEXT NOT NULL, frequency_hz REAL NOT NULL, incidence_deg REAL NOT NULL,
 semimajor_m REAL NOT NULL, semiminor_m REAL NOT NULL, aspect_ratio REAL NOT NULL,
 medium_density_kg_m3 REAL NOT NULL, medium_sound_speed_m_s REAL NOT NULL,
 target_density_kg_m3 REAL, target_sound_speed_m_s REAL,
 source_sha TEXT NOT NULL, software TEXT, method TEXT, qualified INTEGER NOT NULL,
 status TEXT, ts_db REAL, uncertainty_db REAL, finished_at TEXT,
 controls_json TEXT NOT NULL, evidence_zlib BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS reference_physics ON reference_results (physical_id, reference_id);
CREATE INDEX IF NOT EXISTS reference_selection ON reference_results
 (semimajor_m, aspect_ratio, boundary, incidence_deg, frequency_hz);
CREATE TABLE IF NOT EXISTS reference_artifacts (
 reference_fingerprint TEXT NOT NULL REFERENCES reference_results,
 artifact_id INTEGER NOT NULL REFERENCES artifacts,
 PRIMARY KEY (reference_fingerprint, artifact_id));
CREATE TABLE IF NOT EXISTS comparisons (
 comparison_id TEXT PRIMARY KEY, observation_id TEXT NOT NULL REFERENCES observations,
 reference_fingerprint TEXT, reference_id TEXT NOT NULL, reference_source_sha TEXT,
 reference_ts_db REAL, reference_uncertainty_db REAL, delta_ts_db REAL,
 qualification_json TEXT, artifact_id INTEGER NOT NULL REFERENCES artifacts);
CREATE INDEX IF NOT EXISTS comparisons_observation ON comparisons (observation_id);
CREATE VIEW IF NOT EXISTS measurements AS
 SELECT o.*, c.physical_id, c.software, c.boundary, c.frequency_hz, c.incidence_deg,
 c.semimajor_m, c.semiminor_m, c.aspect_ratio, c.medium_density_kg_m3,
 c.medium_sound_speed_m_s, c.target_density_kg_m3, c.target_sound_speed_m_s,
 c.repetition, c.k0a, c.h0, c.hs, c.warmup, c.controls_json
 FROM observations o JOIN cases c USING (case_id) WHERE c.applicable = 1;
CREATE VIEW IF NOT EXISTS latest_measurements AS
 SELECT * FROM measurements m WHERE m.observation_id = (
   SELECT o.observation_id FROM observations o
   LEFT JOIN invocations i ON i.platform=o.platform AND i.invocation_id=o.invocation_id
   WHERE o.platform=m.platform AND o.case_id=m.case_id
   ORDER BY COALESCE(o.finished_at, i.started_at, '') DESC,
     COALESCE(o.origin_run_id, 0) DESC, o.observation_id DESC LIMIT 1);
"""


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _blob(value):
    return zlib.compress(_json(value).encode("utf-8"))


def connect_database(path):
    """Open a disk-backed database with a bounded page cache and indexed views."""
    connection = sqlite3.connect(path, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA cache_size=-8192")
    connection.execute("PRAGMA temp_store=FILE")
    connection.executescript(SCHEMA)
    version = connection.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
    if version is not None and version[0] != str(SCHEMA_VERSION):
        connection.close()
        raise ValueError("Unsupported benchmark database schema")
    connection.execute("INSERT OR IGNORE INTO metadata VALUES ('schema_version', ?)", (str(SCHEMA_VERSION),))
    return connection


def _stream_items(archive, name, prefix, *, omit=()):
    """Read array entries or an object without materializing other document fields."""
    import ijson
    from ijson.common import ObjectBuilder

    if not omit:
        with archive.open(name) as source:
            yield from ijson.items(source, prefix, use_float=True)
        return
    with archive.open(name) as source:
        builder = None
        depth = skip = 0
        for current, event, value in ijson.parse(source, use_float=True):
            if builder is None:
                if current != prefix or event in ("map_key", "end_map", "end_array"):
                    continue
                if event not in ("start_map", "start_array"):
                    yield value
                    continue
                builder = ObjectBuilder()
                depth = 0
            if skip == -1:
                skip = 1 if event in ("start_map", "start_array") else 0
                continue
            if skip > 0:
                if event in ("start_map", "start_array"):
                    skip += 1
                elif event in ("end_map", "end_array"):
                    skip -= 1
                continue
            if event == "map_key" and value in omit:
                # The next value can be a scalar or arbitrarily large container.
                skip = -1
                continue
            builder.event(event, value)
            if event in ("start_map", "start_array"):
                depth += 1
            elif event in ("end_map", "end_array"):
                depth -= 1
            if depth == 0:
                yield builder.value
                builder = None


def _object(archive, name, *, omit=()):
    values = _stream_items(archive, name, "", omit=omit)
    value = next(values, None)
    if not isinstance(value, dict) or next(values, None) is not None:
        raise ValueError(f"Invalid object in {name}")
    return value


def _sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _positive_integer(value):
    if value is None:
        return None
    if isinstance(value, bool) or not str(value).isdigit() or int(value) < 1:
        raise ValueError("Invalid workflow run or artifact identifier")
    return int(value)


def _platform(name, manifest=None):
    for platform in PLATFORMS.values():
        if re.search(rf"(?:^|-){re.escape(platform)}(?:-|$)", name):
            return platform
    return PLATFORMS.get((manifest or {}).get("platform"))


def _files(archive):
    if len(archive.infolist()) > MAX_ARCHIVE_ENTRIES:
        raise ValueError("Too many artifact entries")
    files = {}
    for entry in archive.infolist():
        path = PurePosixPath(entry.filename)
        if (path.is_absolute() or ".." in path.parts or "\\" in entry.filename or ":" in entry.filename
                or ((entry.external_attr >> 16) & 0o170000) == 0o120000):
            raise ValueError("Unsafe artifact path")
        if entry.is_dir():
            continue
        if entry.filename in files or entry.file_size > MAX_ENTRY_BYTES:
            raise ValueError("Duplicate or oversized artifact entry")
        files[entry.filename] = entry
    return files


def _find(files, basename):
    matches = [name for name in files if PurePosixPath(name).name == basename]
    if len(matches) > 1:
        raise ValueError(f"Ambiguous artifact metadata: {basename}")
    return next(iter(matches), None)


def _manifest(connection, manifest, artifact_id):
    identity = {key: manifest[key] for key in MANIFEST_FIELDS if key in manifest}
    if (not set(MANIFEST_FIELDS[:-1]) <= identity.keys()
            or fingerprint(identity) != manifest.get("fingerprint")):
        raise ValueError("Invalid historical timing manifest fingerprint")
    key = manifest["fingerprint"]
    compact = dict(identity, fingerprint=key)
    connection.execute("INSERT OR IGNORE INTO manifests VALUES (?, ?, ?, ?)",
        (key, PLATFORMS.get(manifest.get("platform")), _json(manifest["sources"]), _blob(compact)))
    connection.execute("INSERT OR IGNORE INTO artifact_manifests VALUES (?, ?)", (artifact_id, key))
    return key


def _environment(connection, platform, invocation_id, value, observation=None):
    observation = observation or {}
    evidence = {key: item for key, item in value.items() if key not in ("started_at", "references_fingerprint")}
    key = fingerprint(evidence)
    if PLATFORMS.get(evidence.get("platform")) != platform:
        raise ValueError("Invocation environment platform differs from artifact platform")
    connection.execute("INSERT OR IGNORE INTO environments VALUES (?, ?)", (key, _blob(evidence)))
    row = (platform, invocation_id, key, _positive_integer(evidence.get("github_run_id")),
           _positive_integer(evidence.get("github_run_attempt")), observation.get("started_at", value.get("started_at")))
    previous = connection.execute("SELECT * FROM invocations WHERE platform=? AND invocation_id=?", row[:2]).fetchone()
    if previous is not None and tuple(previous) != row:
        raise ValueError("Conflicting original invocation evidence")
    connection.execute("INSERT OR IGNORE INTO invocations VALUES (?, ?, ?, ?, ?, ?)", row)


def _proof(connection, proof, artifact_id):
    if not isinstance(proof, dict) or not isinstance(proof.get("manifest"), dict):
        raise ValueError("Invalid historical source provenance")
    manifest = proof["manifest"]
    _manifest(connection, manifest, artifact_id)
    platform = PLATFORMS.get(manifest.get("platform"))
    environments = proof.get("environments", {})
    for invocation, observation in environments.get("invocations", {}).items():
        key = observation.get("environment_fingerprint")
        evidence = environments.get("evidence", {}).get(key)
        if not isinstance(evidence, dict) or fingerprint(evidence) != key:
            raise ValueError("Invalid historical environment fingerprint")
        _environment(connection, platform, invocation, evidence, observation)


def _case(connection, row):
    case = {key: row[key] for key in CASE_FIELDS if key in row}
    if (not set(PHYSICAL_FIELDS) <= case.keys() or not isinstance(case.get("controls"), dict)
            or physical_identity(case) != case.get("physical_id") or fingerprint(case) != row.get("case_id")):
        raise ValueError("Case inputs differ from their saved identity")
    for key in PHYSICAL_FIELDS:
        if key == "boundary" or case[key] is None:
            continue
        if not isinstance(case[key], (int, float)) or isinstance(case[key], bool) or not math.isfinite(case[key]):
            raise ValueError("Invalid physical input")
    software = case["software"]
    applicable = int(case["boundary"] in CAPABILITIES.get(software, (case["boundary"],)))
    values = (row["case_id"], case["physical_id"], software, case["boundary"], case["frequency_hz"],
        case["incidence_deg"], case["semimajor_m"], case["semiminor_m"], case["aspect_ratio"],
        case["medium_density_kg_m3"], case["medium_sound_speed_m_s"], case["target_density_kg_m3"],
        case["target_sound_speed_m_s"], case["repetition"], case.get("k0a"), case.get("h0"),
        case.get("hs"), int(case["warmup"]) if "warmup" in case else None,
        _json(case["controls"]), _json(case), applicable)
    previous = connection.execute("SELECT case_json FROM cases WHERE case_id=?", (row["case_id"],)).fetchone()
    if previous is not None and previous[0] != values[-2]:
        raise ValueError("Conflicting inputs for the same case")
    connection.execute("INSERT OR IGNORE INTO cases VALUES (" + ",".join("?" for _ in values) + ")", values)
    return case


def _comparison(connection, row, observation_id, artifact_id):
    if not row.get("reference_id"):
        if row.get("delta_ts_db") is not None:
            raise ValueError("A TS difference lacks its reference identity")
        return
    delta = row.get("delta_ts_db")
    if delta is not None:
        if (row.get("status") != "ok" or not isinstance(row.get("ts_db"), (float, int))
                or not isinstance(row.get("reference_ts_db"), (float, int))
                or not math.isclose(delta, row["ts_db"] - row["reference_ts_db"], rel_tol=1e-12, abs_tol=1e-10)):
            raise ValueError("TS difference is inconsistent with its measured reference")
    summary = {key: row.get(key) for key in ("reference_fingerprint", "reference_id", "reference_source_sha",
        "reference_ts_db", "reference_uncertainty_db", "reference_qualification", "delta_ts_db")}
    identity = fingerprint(dict(observation_id=observation_id, reference=summary))
    values = (identity, observation_id, summary["reference_fingerprint"], summary["reference_id"],
        summary["reference_source_sha"], summary["reference_ts_db"], summary["reference_uncertainty_db"],
        delta, _json(summary["reference_qualification"]), artifact_id)
    connection.execute("INSERT OR IGNORE INTO comparisons VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", values)


def _observation(connection, row, platform, artifact_id, run_id, raw=None, expected_source=None):
    if row.get("status", "pending") == "pending" and raw is not None:
        row = dict(row, **raw)
    case = _case(connection, row)
    source = row.get("source_sha", expected_source)
    if row.get("status", "pending") == "pending":
        return None
    if row.get("status") not in STATUSES or not isinstance(source, str) or not re.fullmatch(r"[a-f0-9]{40}", source):
        raise ValueError("Invalid observation status or source revision")
    if expected_source and source != expected_source:
        raise ValueError("Observed software revision differs from its manifest")
    record = {key: value for key, value in row.items() if key not in CASE_FIELDS
        and key not in ("case_id", "delta_ts_db", "measurement_role", "retained_from_run_id", "measurement_origin_run_id")
        and not key.startswith("reference_")}
    if raw is not None:
        for key, value in raw.items():
            if key in ("measurement_origin_run_id",):
                continue
            if key not in row or row[key] != value:
                raise ValueError("Exported observation differs from its atomic measurement record")
    digest = fingerprint(record)
    invocation_id = row.get("invocation_id")
    if invocation_id is not None and not isinstance(invocation_id, str):
        raise ValueError("Invalid invocation identifier")
    identity = fingerprint(dict(platform=platform, case_id=row["case_id"], source_sha=source,
        invocation_id=invocation_id or f"legacy:{digest}"))
    environment = connection.execute("SELECT * FROM invocations WHERE platform=? AND invocation_id=?",
                                      (platform, invocation_id)).fetchone()
    explicit_origin = _positive_integer(row.get("measurement_origin_run_id"))
    origin = environment["origin_run_id"] if environment and environment["origin_run_id"] else explicit_origin
    attempt = environment["origin_run_attempt"] if environment else None
    # An extend historically records the source artifact run as origin. Its
    # invocation can prove an earlier true measurement run, which takes priority.
    previous = connection.execute("SELECT record_sha256 FROM observations WHERE observation_id=?", (identity,)).fetchone()
    if previous is not None and previous[0] != digest:
        raise ValueError("Conflicting measurements for the same case, software revision, and invocation")
    values = (identity, row["case_id"], platform, source, invocation_id, origin, attempt, row.get("finished_at"),
        row["status"], row.get("elapsed_seconds"), row.get("wall_seconds"), row.get("warmup_seconds"),
        row.get("ts_db"), row.get("amplitude_real_m"), row.get("amplitude_imag_m"),
        row.get("memory_delta_peak_bytes"), row.get("memory_baseline_rss_bytes"), row.get("memory_peak_rss_bytes"),
        row.get("memory_method"), row.get("memory_status"), row.get("memory_samples"),
        row.get("memory_sample_interval_seconds"), digest, _blob(record))
    connection.execute("INSERT OR IGNORE INTO observations VALUES (" + ",".join("?" for _ in values) + ")", values)
    if environment:
        connection.execute("UPDATE observations SET origin_run_id=?, origin_run_attempt=? WHERE observation_id=?",
                           (origin, attempt, identity))
    connection.execute("INSERT OR IGNORE INTO observation_artifacts VALUES (?, ?)", (identity, artifact_id))
    _comparison(connection, row, identity, artifact_id)
    return identity


def _reference(connection, row, artifact_id):
    if (not isinstance(row, dict) or physical_identity(row) != row.get("physical_id")
            or not isinstance(row.get("signature"), dict)
            or fingerprint(row["signature"]) != row.get("reference_id")):
        raise ValueError("Reference inputs differ from their saved identity")
    signature = row["signature"]
    if (signature.get("source_sha") != row.get("source_sha")
            or signature.get("controls") != row.get("controls")
            or signature.get("method") != row.get("method")
            or physical_identity(signature.get("physical", {})) != row["physical_id"]):
        raise ValueError("Reference outputs differ from their signed inputs")
    if row.get("qualified") is True:
        if (row.get("status") != "ok" or not isinstance(row.get("ts_db"), (int, float))
                or not math.isfinite(row["ts_db"]) or not isinstance(row.get("uncertainty_db"), (int, float))
                or row["uncertainty_db"] <= 0 or not row.get("qualification")):
            raise ValueError("Qualified reference lacks finite TS or qualification evidence")
    key = fingerprint(row)
    if connection.execute("SELECT 1 FROM reference_results WHERE reference_fingerprint=?", (key,)).fetchone() is None:
        values = (key, row["reference_id"], row["physical_id"], row["boundary"], row["frequency_hz"],
            row["incidence_deg"], row["semimajor_m"], row["semiminor_m"],
            row.get("aspect_ratio", row["semimajor_m"] / row["semiminor_m"]),
            row["medium_density_kg_m3"], row["medium_sound_speed_m_s"], row["target_density_kg_m3"],
            row["target_sound_speed_m_s"], row["source_sha"], row.get("software"),
            row.get("method"), int(row.get("qualified") is True), row.get("status"), row.get("ts_db"),
            row.get("uncertainty_db"), row.get("finished_at"), _json(row.get("controls", {})), _blob(row))
        connection.execute("INSERT INTO reference_results VALUES (" + ",".join("?" for _ in values) + ")", values)
    connection.execute("INSERT OR IGNORE INTO reference_artifacts VALUES (?, ?)", (key, artifact_id))


@contextmanager
def _transaction(connection):
    connection.execute("SAVEPOINT artifact_import")
    try:
        yield
    except BaseException:
        connection.execute("ROLLBACK TO artifact_import")
        connection.execute("RELEASE artifact_import")
        raise
    else:
        connection.execute("RELEASE artifact_import")


def import_artifact(connection, archive_path, artifact_metadata, repository):
    """Atomically import an artifact, deduplicating copied records across runs.

    Arrays are parsed incrementally. Source revisions, original measurement
    timestamps, invocations, and references remain distinct. Historical adapter
    versions are accepted because collation does not execute numerical software.
    """
    metadata = dict(artifact_metadata)
    artifact_id = _positive_integer(metadata["id"])
    run_id = _positive_integer(metadata.get("run_id", metadata.get("workflow_run", {}).get("id")))
    name = metadata["name"]
    digest = _sha256(archive_path)
    if metadata.get("archive_sha256", digest) != digest:
        raise ValueError("Downloaded artifact does not match its expected digest")
    existing = connection.execute("SELECT archive_sha256, repository FROM artifacts WHERE artifact_id=?", (artifact_id,)).fetchone()
    if existing:
        if tuple(existing) != (digest, repository):
            raise ValueError("An imported artifact identifier has different content or repository")
        return dict(imported=False, skipped=True, artifact_id=artifact_id, observations=0, references=0, cases=0)
    before = database_summary(connection)
    with _transaction(connection), zipfile.ZipFile(archive_path) as archive:
        files = _files(archive)
        manifest_name = _find(files, "manifest.json")
        result_name = _find(files, "results.json")
        sidecar_name = _find(files, "retained-results.json")
        baseline_name = _find(files, "refresh-campaign.json")
        source_name = _find(files, "benchmark-source.json")
        reference_names = [entry for entry in files if PurePosixPath(entry).name in
                           ("references.json", "published-references.json")]
        manifest = _object(archive, manifest_name, omit=("reference_requests",)) if manifest_name else None
        baseline = _object(archive, baseline_name) if baseline_name else None
        platform = _platform(name, manifest)
        if baseline and (baseline.get("repository") != repository or baseline.get("platform") != platform):
            raise ValueError("Baseline metadata belongs to another repository or platform")
        if source_name:
            source = _object(archive, source_name)
            if source.get("repository", repository) != repository:
                raise ValueError("Artifact source metadata belongs to another repository")
        if manifest and PLATFORMS.get(manifest["platform"]) != platform:
            raise ValueError("Artifact name and manifest platform disagree")
        if (result_name or sidecar_name) and platform is None:
            raise ValueError("Timing artifact has no identifiable platform")
        if result_name and manifest is None:
            raise ValueError("Timing case table lacks its historical manifest")
        connection.execute("INSERT INTO artifacts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (artifact_id, repository, run_id, name, metadata.get("created_at"), digest,
             datetime.now(timezone.utc).isoformat(timespec="seconds"), _json(metadata)))
        if manifest:
            _manifest(connection, manifest, artifact_id)
        if sidecar_name:
            sidecar_meta = _object(archive, sidecar_name, omit=("rows", "provenance", "reused_records"))
            if sidecar_meta.get("schema_version") != 1:
                raise ValueError("Unsupported comparison metadata schema")
            for proof in _stream_items(archive, sidecar_name, "provenance.item", omit=("reference_requests",)):
                _proof(connection, proof, artifact_id)
        for entry in files:
            path = PurePosixPath(entry)
            if path.parent.name == "environments" and path.suffix == ".json":
                _environment(connection, platform, path.stem, _object(archive, entry))
        expected_cases = set(manifest["cases"]) if manifest else None
        seen = set()
        if result_name:
            prefix = PurePosixPath(result_name).parent
            for row in _stream_items(archive, result_name, "item"):
                identity = row.get("case_id")
                if identity not in expected_cases or identity in seen:
                    raise ValueError("Case table differs from its manifest or repeats a case")
                seen.add(identity)
                software = row.get("software")
                source_sha = manifest["sources"].get("sources", {}).get(software, {}).get("sha")
                raw_name = str(prefix / "records" / f"{identity}.json")
                raw = _object(archive, raw_name) if raw_name in files else None
                _observation(connection, row, platform, artifact_id, run_id, raw=raw, expected_source=source_sha)
                connection.execute("INSERT OR IGNORE INTO planned_cases VALUES (?, ?, ?, ?)",
                                   (artifact_id, identity, platform, source_sha))
            if seen != expected_cases:
                raise ValueError("Case table is incomplete")
        if sidecar_name:
            for row in _stream_items(archive, sidecar_name, "rows.item"):
                _observation(connection, row, platform, artifact_id, run_id)
            # Reused records are an object keyed by case ID, not an array.
            import ijson
            with archive.open(sidecar_name) as source:
                for identity, row in ijson.kvitems(source, "reused_records", use_float=True):
                    if row.get("case_id") != identity:
                        raise ValueError("Reused case key differs from its record")
                    raw_name = str(PurePosixPath(sidecar_name).parent / "records" / f"{identity}.json")
                    raw = _object(archive, raw_name) if raw_name in files else None
                    _observation(connection, row, platform, artifact_id, run_id, raw=raw)
        for reference_name in reference_names:
            for reference in _stream_items(archive, reference_name, "references.item"):
                _reference(connection, reference, artifact_id)
        # Some reference artifacts include individual cache records as well.
        if not reference_names and not result_name and not sidecar_name:
            for entry in files:
                path = PurePosixPath(entry)
                if path.parent.name in ("references", "reference-cache") and re.fullmatch(r"[a-f0-9]{64}\.json", path.name):
                    _reference(connection, _object(archive, entry), artifact_id)
    after = database_summary(connection)
    return dict(imported=True, skipped=False, artifact_id=artifact_id,
        observations=after["observations"] - before["observations"],
        references=after["references"] - before["references"], cases=after["cases"] - before["cases"])


CSV_FIELDS = ("observation_id", "case_id", "physical_id", "platform", "software", "source_sha",
    "origin_run_id", "origin_run_attempt", "invocation_id", "finished_at", "boundary", "semimajor_m",
    "semiminor_m", "aspect_ratio", "incidence_deg", "frequency_hz", "medium_density_kg_m3",
    "medium_sound_speed_m_s", "target_density_kg_m3", "target_sound_speed_m_s", "repetition", "k0a",
    "h0", "hs", "controls_json", "warmup", "status", "elapsed_seconds", "wall_seconds", "warmup_seconds",
    "ts_db", "amplitude_real_m", "amplitude_imag_m", "memory_delta_peak_bytes", "memory_baseline_rss_bytes",
    "memory_peak_rss_bytes", "memory_method", "memory_status", "memory_samples", "memory_sample_interval_seconds")


def export_csv(connection, path):
    """Stream all deduplicated, applicable observations to gzip-compressed CSV."""
    cursor = connection.execute("SELECT " + ",".join(CSV_FIELDS) + " FROM measurements ORDER BY "
        "platform, software, semimajor_m, aspect_ratio, boundary, incidence_deg, frequency_hz, repetition, finished_at")
    count = 0
    with gzip.open(path, "wt", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(CSV_FIELDS)
        for row in cursor:
            writer.writerow(tuple(row))
            count += 1
    return count


COMPARISON_FIELDS = ("comparison_id", "reference_fingerprint", "reference_id", "reference_source_sha",
    "reference_ts_db", "reference_uncertainty_db", "delta_ts_db", "qualification_json")


def export_comparisons_csv(connection, path):
    """Export each measured observation/reference pairing without choosing a version."""
    columns = [f"m.{name}" for name in CSV_FIELDS] + [f"c.{name}" for name in COMPARISON_FIELDS]
    cursor = connection.execute("SELECT " + ",".join(columns) + " FROM measurements m "
        "JOIN comparisons c USING (observation_id) ORDER BY m.platform, m.software, m.semimajor_m, "
        "m.aspect_ratio, m.boundary, m.incidence_deg, m.frequency_hz, m.repetition, m.finished_at, c.comparison_id")
    count = 0
    with gzip.open(path, "wt", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow((*CSV_FIELDS, *COMPARISON_FIELDS))
        for row in cursor:
            writer.writerow(tuple(row))
            count += 1
    return count


def database_summary(connection):
    """Return compact catalog counters without loading measurement tables."""
    names = {"artifacts": "artifacts", "cases": "cases", "observations": "observations",
             "measurements": "measurements", "references": "reference_results", "comparisons": "comparisons"}
    result = dict(schema_version=SCHEMA_VERSION)
    result.update({key: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                   for key, table in names.items()})
    result["software"] = [row[0] for row in connection.execute("SELECT DISTINCT software FROM measurements ORDER BY software")]
    result["platforms"] = [row[0] for row in connection.execute("SELECT DISTINCT platform FROM observations ORDER BY platform")]
    result["semimajor_m"] = [row[0] for row in connection.execute("SELECT DISTINCT semimajor_m FROM cases ORDER BY semimajor_m")]
    result["latest_measurement_at"] = connection.execute("SELECT max(finished_at) FROM observations").fetchone()[0]
    return result
