"""Wait until the supervisor owns the Windows job before launching a solver."""

from pathlib import Path
import subprocess
import sys
import time

gate = Path(sys.argv[1])
deadline = time.monotonic() + 30
while not gate.exists():
    if time.monotonic() >= deadline:
        raise SystemExit("Supervisor did not release the process gate")
    time.sleep(0.01)
raise SystemExit(subprocess.call(sys.argv[2:]))
