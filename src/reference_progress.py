"""Display saved local reference progress in a visible terminal."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time

import psutil

PREFIX = "local-reference-generation"
TERMINAL_STATES = {"completed", "completed_publication_pending", "paused",
                   "stopped_after_error", "stopped_after_worker_failures"}


@contextmanager
def shared_snapshot_reader(path):
    """Allow the controller to atomically replace snapshots during Windows reads."""
    if os.name != "nt":
        with Path(path).open("r", encoding="utf-8") as stream:
            yield stream
        return
    import ctypes
    from ctypes import wintypes
    import msvcrt

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel.CreateFileW
    create_file.argtypes = (wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                            wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE)
    create_file.restype = wintypes.HANDLE
    close_handle = kernel.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL
    # GENERIC_READ, FILE_SHARE_READ | WRITE | DELETE, OPEN_EXISTING.
    handle = create_file(str(Path(path).resolve()), 0x80000000, 0x7, None, 3, 0x80, None)
    if handle == ctypes.c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        descriptor = msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)
    except BaseException:
        close_handle(handle)
        raise
    try:
        stream = os.fdopen(descriptor, "r", encoding="utf-8")
    except BaseException:
        os.close(descriptor)
        raise
    # fdopen owns the descriptor, which owns the original Windows handle.
    with stream:
        yield stream


def read_snapshot(path, fallback=None):
    try:
        with shared_snapshot_reader(path) as stream:
            return json.load(stream)
    except (FileNotFoundError, PermissionError, json.JSONDecodeError):
        return {} if fallback is None else fallback


def timestamp_epoch(value):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except (ValueError, TypeError, AttributeError):
        return None


def duration(seconds):
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def geometry(job):
    boundary = {"rigid": "fixed_rigid", "pressure_release": "pressure_release",
                "gas": "gas_filled", "liquid": "liquid_filled"}.get(job.get("boundary"), job.get("boundary", "unknown"))
    return (f"boundary={boundary} a={job['semimajor_m']:g} m a/b={job['aspect_ratio']:g} "
            f"f={job['frequency_hz'] / 1000:.2f} kHz angle={job['incidence_deg']:g} deg")


def controller_alive(state):
    pid = state.get("controller_pid")
    if not pid:
        return True
    try:
        process = psutil.Process(int(pid))
        if not process.is_running():
            return False
        command = " ".join(process.cmdline())
        return "local_references.py" in command
    except (psutil.NoSuchProcess, psutil.ZombieProcess):
        return False
    except (psutil.AccessDenied, PermissionError):
        return True


class ProgressReporter:
    def __init__(self, manifest, heartbeat_seconds=15):
        self.manifest = manifest
        self.jobs = {job["reference_id"]: job for job in manifest["requests"]}
        self.selected = len(self.jobs)
        self.existing = len(manifest.get("skipped_existing_physical_ids", []))
        self.total = manifest.get("planned", self.selected + self.existing)
        self.heartbeat = heartbeat_seconds
        self.previous = None
        self.last_print = None
        self.finished = False

    def update(self, state, marker, now=None, alive=True):
        now = time.time() if now is None else now
        attempted = state.get("attempted", {})
        published = set(state.get("published", []))
        qualified = sum(bool(item.get("qualified")) for item in attempted.values())
        failed = len(attempted) - qualified
        remaining = self.selected - len(attempted)
        pending = max(0, qualified - len(published))
        status = state.get("state", "starting")
        stopped = status not in TERMINAL_STATES and bool(state.get("controller_pid")) and not alive
        if stopped:
            status = "controller_stopped"
        starts = [timestamp_epoch(entry.get("at")) for entry in state.get("history", [])
                  if entry.get("action") in ("start", "resume")]
        start = next((entry for entry in reversed(starts) if entry is not None), None)
        active = None
        if status == "running":
            for candidate in (marker, state.get("current") or {}):
                identity = candidate.get("reference_id")
                if (identity in self.jobs and identity not in attempted
                        and candidate.get("phase") != "recorded"
                        and (start is None or candidate.get("started_epoch", 0) >= start - 1)):
                    active = candidate
                    break
        current = (active.get("reference_id"), active.get("phase")) if active else None
        errors = state.get("publication_errors", [])
        latest_error = errors[-1] if errors else None
        observed = dict(status=status, current=current, attempted=set(attempted), published=published,
                        pending=pending, last_error=state.get("last_error"), publication_error=latest_error)
        previous = self.previous
        messages = []
        if previous is None:
            messages.append(f"PLAN total={self.total} existing={self.existing} to_calculate={self.selected}")
        else:
            for identity in attempted:
                if identity in previous["attempted"]:
                    continue
                record = attempted[identity]
                job = self.jobs.get(identity)
                label = "QUALIFIED" if record.get("qualified") else "FAILED"
                detail = geometry(job) if job else f"reference_id={identity}"
                messages.append(f"{label} status={record.get('status', 'unknown')} | {detail}")
            uploaded = len(published - previous["published"])
            if uploaded:
                messages.append(f"UPLOADED +{uploaded} uploaded={len(published)} pending={pending}")
        if latest_error and (previous is None or latest_error != previous["publication_error"]):
            messages.append(f"UPLOAD FAILED {latest_error.get('error', 'unknown error')} | pending={pending}, saved locally for retry")
        if status == "stopped_after_error" and state.get("last_error") and (previous is None or state["last_error"] != previous["last_error"] or previous["status"] != status):
            messages.append(f"CONTROLLER ERROR {state['last_error']}")
        changed = previous is None or observed != previous
        heartbeat_due = self.last_print is None or now - self.last_print >= self.heartbeat
        if changed or heartbeat_due:
            elapsed = duration(now - start) if start is not None else "not_started"
            detail = ""
            if active:
                detail = (f" | phase={active.get('phase', 'unknown')} "
                          f"case_elapsed={duration(now - active['started_epoch'])} | "
                          f"{geometry(self.jobs[active['reference_id']])}")
            elif status == "waiting_for_memory":
                minimum = self.manifest.get("minimum_available_bytes", 0) / 1024**3
                detail = f" | waiting for at least {minimum:.2f} GiB available RAM"
            messages.append(f"{status.upper()} attempted={len(attempted)}/{self.selected} "
                            f"qualified={qualified} failed={failed} remaining={remaining} "
                            f"uploaded={len(published)} upload_pending={pending} "
                            f"session_elapsed={elapsed}{detail}")
            self.last_print = now
        self.previous = observed
        self.finished = (status in TERMINAL_STATES and (not state.get("controller_pid") or not alive)) or stopped
        timestamp = datetime.fromtimestamp(now, timezone.utc).astimezone().isoformat(timespec="seconds")
        return [f"[{timestamp}] {message}" for message in messages]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--log", type=Path)
    args = parser.parse_args()
    work = args.work.resolve()
    manifest = read_snapshot(work / f"{PREFIX}-manifest.json")
    if not manifest:
        raise SystemExit("No saved local reference manifest found in the supplied workspace")
    reporter = ProgressReporter(manifest)
    log = args.log.open("a", encoding="utf-8", buffering=1) if args.log else None
    print("Local BEM/FEM-BEM reference progress. Updates on each event and every 15 seconds.", flush=True)
    print("Closing this monitor leaves the reference calculation running.", flush=True)
    previous_state = {}
    try:
        while True:
            state = read_snapshot(work / f"{PREFIX}-status.json", previous_state)
            previous_state = state
            marker = read_snapshot(work / "julia-smoke-rigid" / "reference-active.json")
            for line in reporter.update(state, marker, alive=controller_alive(state)):
                print(line, flush=True)
                if log:
                    log.write(line + "\n")
            if reporter.finished:
                break
            time.sleep(1)
    except KeyboardInterrupt:
        print("Progress monitor closed. Reference calculation continues independently.", flush=True)
    finally:
        if log:
            log.close()


if __name__ == "__main__":
    main()
