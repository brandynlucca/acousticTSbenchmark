"""Download newly available checkpoint snapshots for a local progress view."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import shutil
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, help="GitHub owner/repository")
    parser.add_argument("--run", required=True, type=int)
    parser.add_argument("--platform", default="ubuntu-latest")
    parser.add_argument("--output", required=True)
    parser.add_argument("--interval", default=30, type=float)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.interval < 10:
        parser.error("Polling interval must be at least 10 seconds")
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    previous = None
    while True:
        payload = subprocess.check_output(["gh", "api", "--paginate", "--slurp",
            f"repos/{args.repo}/actions/runs/{args.run}/artifacts?per_page=100"], text=True)
        artifacts = [a for page in json.loads(payload) for a in page["artifacts"]
                     if not a["expired"] and (a["name"].startswith(f"checkpoint-{args.platform}-")
                                              or a["name"] == f"results-{args.platform}")]
        if artifacts:
            latest = max(artifacts, key=lambda a: a["id"])
            if latest["id"] != previous:
                # Download into a new snapshot so interrupted transfers cannot damage the last view.
                snapshot = output / str(latest["id"])
                snapshot.mkdir(exist_ok=True)
                subprocess.run(["gh", "run", "download", str(args.run), "--repo", args.repo,
                                "--name", latest["name"], "--dir", str(snapshot)], check=True)
                for name in ("progress.html", "results.csv", "results.json", "status.json", "manifest.json"):
                    source = snapshot / name
                    if source.exists():
                        temporary = output / (name + ".tmp")
                        shutil.copyfile(source, temporary)
                        os.replace(temporary, output / name)
                print(f"Updated {output / 'progress.html'} from {latest['name']}", flush=True)
                previous = latest["id"]
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
