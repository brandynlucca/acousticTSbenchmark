"""Installation isolation, source integrity, and setup process ownership."""

import hashlib
import copy
import json
import os
from pathlib import Path
import sys
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import prepare


class PreparationTests(unittest.TestCase):
    def setUp(self):
        scratch = ROOT / ".scratch/codex/dense-benchmark/tests"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=scratch)
        self.path = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_setup_timeout_terminates_compiler_descendants(self):
        ready, escaped = self.path / "child.ready", self.path / "child.escaped"
        child = ("from pathlib import Path; import time; "
                 f"Path({str(ready)!r}).write_text('ready'); time.sleep(2); "
                 f"Path({str(escaped)!r}).write_text('escaped')")
        parent = ("import subprocess,sys,time; "
                  f"subprocess.Popen([sys.executable,'-c',{child!r}]); time.sleep(30)")
        real_popen = subprocess.Popen

        def start_process(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            communicate = process.communicate
            first_wait = True

            def synchronized_communicate(*args, timeout=None, **kwargs):
                nonlocal first_wait
                if first_wait:
                    first_wait = False
                    deadline = time.monotonic() + 20
                    while not ready.exists() and time.monotonic() < deadline:
                        if process.poll() is not None:
                            break
                        time.sleep(0.01)
                    # Exercise a real TimeoutExpired and real process-tree
                    # cleanup after proving the grandchild started. Interpreter
                    # startup on a loaded Windows host is not the subject here.
                    return communicate(*args, timeout=0.1, **kwargs)
                return communicate(*args, timeout=timeout, **kwargs)

            process.communicate = synchronized_communicate
            return process

        started = time.monotonic()
        with patch.dict(os.environ, {"BENCHMARK_DEADLINE": str(time.time() + 30)}), \
                patch.object(prepare.subprocess, "Popen", side_effect=start_process):
            with self.assertRaises(TimeoutError):
                prepare.execute([sys.executable, "-c", parent], workspace=self.path,
                                timeout_seconds=3)
        self.assertLess(time.monotonic() - started, 25)
        self.assertTrue(ready.exists(), "The child must start before testing tree termination")
        time.sleep(2.1)
        self.assertFalse(escaped.exists(), "A setup child survived the timeout")

    def test_expired_campaign_does_not_start_setup(self):
        output = self.path / "started"
        with patch.dict(os.environ, {"BENCHMARK_DEADLINE": "1"}):
            with self.assertRaisesRegex(TimeoutError, "deadline"):
                prepare.execute([sys.executable, "-c", f"open({str(output)!r},'w').close()"],
                                workspace=self.path)
        self.assertFalse(output.exists())

    def test_reused_source_requires_unchanged_original_files(self):
        source = {"sha": "a" * 40}
        directory = self.path / "software"
        directory.mkdir()
        code = directory / "solver.txt"
        code.write_text("original", encoding="utf-8")
        record = dict(source, files_sha256={"solver.txt": hashlib.sha256(code.read_bytes()).hexdigest()})
        (directory / ".benchmark-source.json").write_text(json.dumps(record), encoding="utf-8")
        self.assertEqual(prepare.obtain_source("software", source, self.path), directory)
        code.write_text("changed", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "modified"):
            prepare.obtain_source("software", source, self.path)

    def test_resume_uses_build_inputs_and_preserves_output_hash_evidence(self):
        first_root, second_root = r"C:\Runner Temp\first", r"D:\Runner Temp\second"

        def evidence(root, output_hash):
            slash = root.replace("\\", "/")
            return {
                "prol_native_sha256": output_hash,
                "r_installed_sha256": {"libs/acousticTS.dll": output_hash, "R/acousticTS.rdb": output_hash},
                "liquid_native_sha256": "unchanged stock binary",
                "acousticts": {"sha": "same source", "archive_sha256": output_hash,
                               "files_sha256": {"solver.cpp": "same source bytes"}},
                "python_packages": "echosms @ file:///" + slash.replace(" ", "%20") + "/echosms\n",
                "r_packages": {"compiler_config": {"FC": root + r"\bin\gfortran", "FFLAGS": "-O2"}},
                "julia_environments": {"acousticscattering": {
                    "Manifest.toml": f'project_hash = "{output_hash}"\njulia_version = "1.12.1"\npath = "{slash}/acousticscattering"\n',
                    "LocalPreferences.toml": "precompile_workload = false\n"}},
            }

        first, second = evidence(first_root, "build-one"), evidence(second_root, "build-two")
        expected = prepare.fingerprint(prepare.compatibility_evidence(first, first_root))
        self.assertEqual(expected, prepare.fingerprint(prepare.compatibility_evidence(second, second_root)))
        self.assertEqual(first["prol_native_sha256"], "build-one")
        self.assertIn("project_hash", first["julia_environments"]["acousticscattering"]["Manifest.toml"])
        changed = copy.deepcopy(second)
        changed["r_packages"]["compiler_config"]["FFLAGS"] = "-O3"
        self.assertNotEqual(expected, prepare.fingerprint(prepare.compatibility_evidence(changed, second_root)))
        changed = copy.deepcopy(second)
        changed["liquid_native_sha256"] = "different stock binary"
        self.assertNotEqual(expected, prepare.fingerprint(prepare.compatibility_evidence(changed, second_root)))

    def fake_source(self, name, source, directory):
        destination = directory / name
        destination.mkdir()
        (destination / ".benchmark-source.json").write_text(json.dumps(source), encoding="utf-8")
        if name == "liquid_spheroid":
            native = destination / ("windows/pro_sphwv.exe" if os.name == "nt" else "linux/pro_sphwv")
            native.parent.mkdir()
            native.write_bytes(b"native fixture")
        return destination

    def test_echosms_installation_failure_preserves_prol(self):
        native = self.path / "native"
        native.write_bytes(b"compiler output fixture")

        def fake_execute(command, **kwargs):
            if str(command[-1]).endswith("echosms") and "install" in command:
                raise RuntimeError("echoSMs installation failed")
            return "fixture dependency evidence"

        with patch.object(prepare, "obtain_source", side_effect=self.fake_source), \
                patch.object(prepare, "execute", side_effect=fake_execute), \
                patch.object(prepare, "compile_prol", return_value=native), \
                patch.object(sys, "argv", ["prepare.py", "--directory", str(self.path),
                                         "--software", "prol_spheroid,echosms"]):
            prepare.main()
        runtime = prepare.read_json(self.path / "runtime.json")
        self.assertEqual(runtime["availability"]["echosms"]["status"], "setup_error")
        self.assertEqual(runtime["availability"]["prol_spheroid"]["status"], "ready")

    def test_liquid_only_does_not_download_or_install_acoustic_scattering(self):
        def fake_execute(command, **kwargs):
            self.assertEqual(command[4], "-")
            project = Path(command[3])
            project.mkdir()
            for filename in ("Project.toml", "Manifest.toml", "LocalPreferences.toml"):
                (project / filename).write_text("fixture = true\n", encoding="utf-8")

        with patch.object(prepare, "obtain_source", side_effect=self.fake_source) as obtain, \
                patch.object(prepare, "execute", side_effect=fake_execute), \
                patch.object(prepare.platform, "system", return_value="Windows" if os.name == "nt" else "Linux"), \
                patch.object(prepare.platform, "machine", return_value="x86_64"), \
                patch.object(sys, "argv", ["prepare.py", "--directory", str(self.path),
                                         "--software", "liquid_spheroid"]):
            prepare.main()
        self.assertEqual([call.args[0] for call in obtain.call_args_list], ["liquid_spheroid"])
        runtime = prepare.read_json(self.path / "runtime.json")
        self.assertEqual(runtime["availability"]["liquid_spheroid"]["status"], "ready")
        self.assertIn("LocalPreferences.toml",
                      runtime["environment_evidence"]["julia_environments"]["liquid_spheroid"])


if __name__ == "__main__":
    unittest.main()
