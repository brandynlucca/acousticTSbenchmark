"""Process fixture for timeout tests. This is never a scientific adapter."""

import json
from pathlib import Path
import sys
import time

request = json.loads(Path(sys.argv[1]).read_text())
if request["controls"]["behavior"] == "startup_sleep":
    time.sleep(30)
else:
    Path(request["timing_started_path"]).write_text(json.dumps({"started_epoch": time.time()}))
    time.sleep(30)
