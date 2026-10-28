"""Publish qualified TS reference curves independently of timing progress."""

import base64
import json
import math
from pathlib import Path
import re

from benchmark import utc_now
from publish_progress import curves_from_rows


FIELDS = ("physical_id", "reference_id", "boundary", "semimajor_m", "aspect_ratio", "incidence_deg",
          "frequency_hz", "k0a", "h0", "ts_db", "uncertainty_db", "method", "source_sha")


def _finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def curves_from_cache(jobs, cache_dir):
    """Recheck cached solver evidence before exposing a standalone curve."""
    from references import cached_reference

    records = {}
    for job in jobs:
        record = cached_reference(Path(cache_dir) / f"{job['reference_id']}.json", job)
        if record is not None:
            records[job["reference_id"]] = record
    return curves_from_records(jobs, records)


def curves_from_records(jobs, records):
    from references import valid_cached

    curves = []
    for job in jobs:
        record = records.get(job["reference_id"])
        if record is None or not valid_cached(record, job):
            continue
        a, b = job["semimajor_m"], job["semiminor_m"]
        k0 = 2 * math.pi * job["frequency_hz"] / job["medium_sound_speed_m_s"]
        curve = {key: record.get(key) for key in FIELDS}
        curve.update(aspect_ratio=job["aspect_ratio"], k0a=k0*a,
                     h0=k0*math.sqrt((a-b)*(a+b)))
        curves.append(curve)
    return curves


def _validated(curves):
    result = {}
    for curve in curves:
        if (not isinstance(curve, dict) or set(curve) != set(FIELDS) or
                any(not isinstance(curve[key], str) or not re.fullmatch(r"[a-f0-9]{64}", curve[key])
                    for key in ("physical_id", "reference_id")) or
                not isinstance(curve["source_sha"], str) or not re.fullmatch(r"[a-f0-9]{40}", curve["source_sha"]) or
                curve["boundary"] not in ("gas", "liquid", "rigid", "pressure_release") or
                curve["method"] != ("fem_bem" if curve["boundary"] in ("gas", "liquid") else "bem") or
                any(not _finite(curve[key]) for key in
                    ("semimajor_m", "aspect_ratio", "incidence_deg", "frequency_hz", "k0a", "h0", "ts_db", "uncertainty_db")) or
                min(curve["semimajor_m"], curve["frequency_hz"], curve["k0a"], curve["h0"], curve["uncertainty_db"]) <= 0 or
                curve["aspect_ratio"] <= 1 or not 0 <= curve["incidence_deg"] <= 90):
            raise ValueError("Invalid qualified TS reference curve")
        previous = result.setdefault(curve["physical_id"], curve)
        if previous != curve:
            raise ValueError("Conflicting qualified TS references")
    return result


def publish_curves(api, run_id, curves):
    """Merge reference shards without modifying any platform timing snapshots."""
    from publish_progress import APIError, BRANCH, MAX_SNAPSHOT_BYTES

    if type(run_id) is not int or run_id <= 0:
        raise ValueError("Reference curves require a positive run ID")
    return _publish_curves(api, f"reference-curves-{run_id}.json", curves, run_id)


def publish_shared_curves(api, curves):
    """Publish independently qualified references for reuse across timing runs."""
    return _publish_curves(api, "qualified-reference-curves.json", curves, None)


def _publish_curves(api, filename, curves, run_id):
    from publish_progress import APIError, BRANCH, MAX_SNAPSHOT_BYTES

    additions = _validated(curves)
    if not additions:
        return 0
    for _ in range(4):
        sha, existing = None, {}
        try:
            entry = api.request("GET", f"/contents/{filename}?ref={BRANCH}")
            sha = entry["sha"]
            value = entry
            if entry.get("encoding") == "none" or not entry.get("content"):
                value = api.request("GET", f"/git/blobs/{sha}")
            if value.get("encoding") != "base64" or value.get("size", 0) > MAX_SNAPSHOT_BYTES:
                raise ValueError("Unsupported reference curve snapshot")
            raw = base64.b64decode("".join(value["content"].split()), validate=True)
            if len(raw) > MAX_SNAPSHOT_BYTES:
                raise ValueError("Oversized reference curve snapshot")
            current = json.loads(raw)
            if current.get("schema_version") != 1 or current.get("run_id") != run_id:
                raise ValueError("Reference curve snapshot belongs to a different run")
            existing = _validated(current["reference_curves"])
        except APIError as error:
            if error.status != 404:
                raise
        merged = _validated([*existing.values(), *additions.values()])
        if existing == merged:
            return 0
        payload = dict(schema_version=1, run_id=run_id, updated_at=utc_now(),
                       reference_curves=sorted(merged.values(), key=lambda item: item["physical_id"]))
        raw = json.dumps(payload, allow_nan=False, separators=(",", ":")).encode()
        if len(raw) > MAX_SNAPSHOT_BYTES:
            raise ValueError("Oversized reference curve snapshot")
        body = dict(message=f"Update qualified TS reference curves for run {run_id}" if run_id is not None else "Update shared qualified TS reference curves", branch=BRANCH,
                    content=base64.b64encode(raw).decode())
        if sha:
            body["sha"] = sha
        try:
            api.request("PUT", f"/contents/{filename}", body)
            return len(merged) - len(existing)
        except APIError as error:
            if error.status not in (409, 422):
                raise
    raise RuntimeError("Reference curves changed concurrently too often")
