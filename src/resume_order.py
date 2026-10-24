"""Change only campaign scheduling while retaining source-pinned measurements."""

import argparse
import copy
import csv
import json
import os
from pathlib import Path
import sys

# Actions preserves this controller outside the source-pinned checkout.
sys.path.insert(0, str(Path.cwd() / "src"))
from benchmark import STATUSES, atomic_json, build_cases, fingerprint, read_json, utc_now
from campaign import IDENTITY_FIELDS
from report import render_report


def change_order(directory, configuration, profile, software, repetitions, order):
    """Validate an identical case set, then retain records under a new ordering."""
    if order not in ("boundary_major", "frequency_round_robin"):
        raise ValueError("Unknown resume order")
    directory = Path(directory)
    if (directory / "running.lock").exists():
        raise ValueError("Cannot change order while a timing process may be active")
    revised = copy.deepcopy(configuration)
    revised["case_order"] = order
    original_cases = build_cases(configuration, profile, software, repetitions)
    cases = build_cases(revised, profile, software, repetitions)
    by_id = {case["case_id"]: case for case in cases}
    if by_id != {case["case_id"]: case for case in original_cases}:
        raise ValueError("Resume order would change prediction inputs")
    manifest_path = directory / "manifest.json"
    if not manifest_path.exists():
        if (directory / "results.json").exists() or any((directory / "records").glob("*.json")):
            raise ValueError("Saved measurements require a manifest")
        return revised, 0
    manifest = read_json(manifest_path)
    identity = {key: manifest[key] for key in IDENTITY_FIELDS}
    if "retained_fingerprint" in manifest:
        identity["retained_fingerprint"] = manifest["retained_fingerprint"]
    if (fingerprint(identity) != manifest.get("fingerprint") or
            manifest["configuration"] != configuration or manifest["profile"] != profile or
            manifest["cases"] != [case["case_id"] for case in original_cases]):
        raise ValueError("Saved manifest does not match the original campaign")
    previous_rows = read_json(directory / "results.json")
    if [row.get("case_id") for row in previous_rows] != manifest["cases"]:
        raise ValueError("Saved rows do not match the manifest order")
    rows_by_id = {row["case_id"]: row for row in previous_rows}
    records = {path.stem: read_json(path) for path in (directory / "records").glob("*.json")}
    if records.keys() - by_id.keys():
        raise ValueError("Saved measurements include unknown cases")
    rows = []
    for case in cases:
        row, record = rows_by_id[case["case_id"]], records.get(case["case_id"])
        if (any(row.get(key) != value for key, value in case.items()) or
                row.get("status") not in STATUSES | {"pending"} or
                (row["status"] != "pending" and record is None)):
            raise ValueError("Saved prediction inputs or measurements are inconsistent")
        if record is not None:
            if (record.get("case_id") != case["case_id"] or record.get("status") not in STATUSES or
                    record.get("source_sha") != manifest["sources"]["sources"][case["software"]]["sha"] or
                    any(record[key] != case[key] for key in record.keys() & case.keys()) or
                    (row["status"] != "pending" and any(row.get(key) != value for key, value in record.items()))):
                raise ValueError("Saved measurement does not match its prediction and source")
            # Cancellation can leave atomic records newer than the last table.
            row = dict(row, **record)
        rows.append(row)
    if revised == configuration:
        return revised, len(records)
    updated = dict(manifest, configuration=revised, cases=[case["case_id"] for case in cases])
    updated_identity = {key: updated[key] for key in IDENTITY_FIELDS}
    if "retained_fingerprint" in updated:
        updated_identity["retained_fingerprint"] = updated["retained_fingerprint"]
    updated["fingerprint"] = fingerprint(updated_identity)
    updated["order_changes"] = [*manifest.get("order_changes", []), dict(
        changed_at=utc_now(), from_order=configuration.get("case_order", "boundary_major"),
        to_order=order, previous_fingerprint=manifest["fingerprint"], preserved_records=len(records),
        workflow_sha=os.getenv("GITHUB_SHA"), run_id=os.getenv("GITHUB_RUN_ID"))]
    status = dict(state="complete" if len(records) == len(cases) else "budget_paused",
                  updated_at=utc_now(), planned=len(cases), recorded=len(records),
                  outcomes={state: sum(row["status"] == state for row in rows)
                            for state in sorted({row["status"] for row in rows})})
    # Every validation precedes writes. Original per-case record files are untouched.
    atomic_json(directory / "results.json", rows)
    temporary = directory / "results.csv.tmp"
    with temporary.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(dict.fromkeys(key for row in rows for key in row)))
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value
                             for key, value in row.items()})
    os.replace(temporary, directory / "results.csv")
    atomic_json(directory / "status.json", status)
    render_report(directory / "progress.html", rows, status)
    atomic_json(manifest_path, updated)
    return revised, len(records)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--software", required=True)
    parser.add_argument("--repetitions", type=int, required=True)
    parser.add_argument("--order", choices=("boundary_major", "frequency_round_robin"), required=True)
    args = parser.parse_args()
    configuration, count = change_order(args.directory, read_json(args.config), args.profile,
                                       args.software.split(","), args.repetitions, args.order)
    atomic_json(args.config, configuration)
    print(f"Resume order: {args.order}. Preserved {count} measured cases. Only pending cases will run.", flush=True)


if __name__ == "__main__":
    main()
