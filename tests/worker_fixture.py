"""Process fixture for timeout and memory tests, never a scientific adapter."""

import gc
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def resident_buffer(size):
    value = bytearray(size)
    # Commit every page rather than relying on lazy allocation of zero pages.
    value[::4096] = b"x" * len(value[::4096])
    return value


if sys.argv[1] == "--memory-child":
    import psutil

    identities = [dict(pid=pid, created_at=psutil.Process(pid).create_time())
                  for pid in (os.getppid(), os.getpid())]
    (Path(sys.argv[2]).parent / "child-pids.json").write_text(json.dumps(identities))
    allocation = resident_buffer(48 * 1024 * 1024)
    Path(sys.argv[2]).touch()
    time.sleep(float(sys.argv[3]))
    raise SystemExit(0)

request = json.loads(Path(sys.argv[1]).read_text())
behavior = request["controls"]["behavior"]
if behavior == "startup_sleep":
    time.sleep(30)
else:
    if behavior.startswith("memory"):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
        from worker_python import memory_boundary
        warmup = resident_buffer(32 * 1024 * 1024)
        memory_boundary(request, "ready")
    Path(request["timing_started_path"]).write_text(json.dumps({"started_epoch": time.time()}))
    started = time.perf_counter()
    if behavior in ("memory_child", "memory_child_timeout"):
        ready = Path(request["work_directory"]) / "child.ready"
        child = subprocess.Popen([sys.executable, __file__, "--memory-child", str(ready),
                                  "30" if behavior.endswith("timeout") else ".3"])
        child.wait()
    elif behavior.startswith("memory"):
        allocation = resident_buffer(32 * 1024 * 1024)
        if behavior == "memory_release":
            time.sleep(.15)
            del allocation
            gc.collect()
    else:
        time.sleep(30)
    elapsed = time.perf_counter() - started
    if behavior.startswith("memory"):
        memory_boundary(request, "done")
        # This much larger buffer is deliberately outside the prediction.
        postprocessing = resident_buffer(96 * 1024 * 1024)
        time.sleep(.03)
    Path(sys.argv[2]).write_text(json.dumps(dict(status="ok", elapsed_seconds=elapsed, ts_db=-40)))
