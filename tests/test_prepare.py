"""Installation isolation, source integrity, and setup process ownership."""

import hashlib
import copy
import io
import json
import os
from pathlib import Path
import sys
import subprocess
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
import zipfile

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

    def test_posix_timeout_does_not_signal_reaped_group_again(self):
        process = SimpleNamespace(
            pid=12345,
            communicate=Mock(side_effect=[subprocess.TimeoutExpired("compiler", 3), ("", None)]),
            poll=Mock(return_value=-9),
            returncode=-9,
        )
        # macOS can report EPERM when cleanup signals a group ID that has
        # already been terminated and reaped. That must not mask the timeout.
        killpg = Mock(side_effect=[None, PermissionError(1, "Operation not permitted")])
        posix_os = SimpleNamespace(**dict(vars(os), name="posix", killpg=killpg))
        with patch.object(prepare, "os", posix_os), \
                patch.object(prepare, "signal", SimpleNamespace(SIGKILL=9)), \
                patch.object(prepare.subprocess, "Popen", return_value=process):
            with self.assertRaisesRegex(TimeoutError, "Setup command exceeded 3.0 seconds"):
                prepare.execute(["compiler"], workspace=self.path, timeout_seconds=3)
        killpg.assert_called_once_with(process.pid, 9)
        self.assertEqual(process.communicate.call_count, 2)

    def test_posix_completed_setup_still_terminates_descendants(self):
        process = SimpleNamespace(
            pid=12345, communicate=Mock(return_value=("complete", None)),
            poll=Mock(return_value=0), returncode=0,
        )
        for error in (None, PermissionError(1, "Operation not permitted")):
            with self.subTest(permission_error=error is not None):
                killpg = Mock(side_effect=error)
                posix_os = SimpleNamespace(**dict(vars(os), name="posix", killpg=killpg))
                with patch.object(prepare, "os", posix_os), \
                        patch.object(prepare, "signal", SimpleNamespace(SIGKILL=9)), \
                        patch.object(prepare.subprocess, "Popen", return_value=process):
                    if error:
                        with self.assertRaises(PermissionError):
                            prepare.execute(["compiler"], workspace=self.path)
                    else:
                        self.assertEqual(prepare.execute(["compiler"], workspace=self.path), "complete")
                killpg.assert_called_once_with(process.pid, 9)

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

    def test_archive_preserves_configure_execution_mode_and_source_bytes(self):
        script = b"#!/bin/sh\nprintf 'configure fixture passed\\n'\n"
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as output:
            for name, mode, content in (("configure", 0o104755, script),
                                        ("README", 0o100644, b"source fixture\n")):
                member = zipfile.ZipInfo("source-root/" + name)
                member.create_system = 3
                member.external_attr = mode << 16
                output.writestr(member, content)
        source = dict(repository="owner/repository", sha="b" * 40)
        applied = {}
        actual_chmod = Path.chmod

        def chmod(path, mode):
            applied[path.name] = mode
            if os.name == "posix":
                actual_chmod(path, mode)

        # Exercise the POSIX extraction branch on Windows too. POSIX CI also
        # runs the extracted script and verifies its real filesystem mode.
        posix_os = SimpleNamespace(**dict(vars(os), name="posix"))
        with patch.object(prepare.urllib.request, "urlopen", return_value=io.BytesIO(archive.getvalue())), \
                patch.object(prepare, "os", posix_os), patch.object(Path, "chmod", new=chmod):
            destination = prepare.obtain_source("software", source, self.path)
        self.assertEqual(applied, {"configure": 0o755, "README": 0o644})
        self.assertEqual((destination / "configure").read_bytes(), script)
        evidence = prepare.read_json(destination / ".benchmark-source.json")
        self.assertEqual(evidence["files_sha256"]["configure"], hashlib.sha256(script).hexdigest())
        if os.name == "posix":
            self.assertEqual((destination / "configure").stat().st_mode & 0o7777, 0o755)
            completed = subprocess.run([str(destination / "configure")], cwd=destination,
                                       capture_output=True, text=True, check=True, timeout=5)
            self.assertEqual(completed.stdout, "configure fixture passed\n")

    def test_archive_still_rejects_symbolic_links(self):
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as output:
            member = zipfile.ZipInfo("source-root/configure")
            member.create_system = 3
            member.external_attr = 0o120777 << 16
            output.writestr(member, "../outside")
        with patch.object(prepare.urllib.request, "urlopen", return_value=io.BytesIO(archive.getvalue())):
            with self.assertRaisesRegex(ValueError, "symbolic links"):
                prepare.obtain_source("software", dict(repository="owner/repository", sha="c" * 40), self.path)
        self.assertFalse((self.path / "software/configure").exists())

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
                expected = "0.0.0+g" + prepare.read_json(ROOT / "sources.lock.json")["sources"]["echosms"]["sha"][:12]
                self.assertEqual(kwargs["env"]["SETUPTOOLS_SCM_PRETEND_VERSION"], expected)
                self.assertNotIn("SETUPTOOLS_SCM_PRETEND_VERSION", os.environ)
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

    def test_liquid_legacy_library_is_verified_and_scoped_to_native_loading(self):
        package = b"verified package fixture"
        package_hash = hashlib.sha256(package).hexdigest()
        native = self.path / "pro_sphwv"
        native.write_bytes(b"unchanged upstream executable")
        native_hash = hashlib.sha256(native.read_bytes()).hexdigest()
        calls = []

        def fake_execute(command, **kwargs):
            calls.append((command, kwargs))
            if command[0] == "dpkg-deb":
                library = Path(command[-1]) / "usr/lib/x86_64-linux-gnu/libmpfr.so.4"
                library.parent.mkdir(parents=True)
                library.write_bytes(b"real ABI 4 library fixture")
                return None
            self.assertEqual(command, ["ldd", "-r", native])
            self.assertEqual(kwargs["env"]["LD_LIBRARY_PATH"],
                             str(self.path / "liquid-runtime/usr/lib/x86_64-linux-gnu") + ":/existing")
            return "libmpfr.so.4 => /fixture/libmpfr.so.4 (0x12345678)\n"

        with patch.object(prepare, "LIQUID_MPFR_SHA256", package_hash), \
                patch.object(prepare.urllib.request, "urlopen", return_value=io.BytesIO(package)), \
                patch.object(prepare, "execute", side_effect=fake_execute), \
                patch.dict(os.environ, {"LD_LIBRARY_PATH": "/existing"}):
            path, evidence = prepare.liquid_linux_libraries(native, self.path)
            self.assertEqual(os.environ["LD_LIBRARY_PATH"], "/existing")
        self.assertEqual(path, str(self.path / "liquid-runtime/usr/lib/x86_64-linux-gnu"))
        self.assertEqual(evidence["package_sha256"], package_hash)
        self.assertNotIn("0x", evidence["loader_dependencies"])
        self.assertEqual(hashlib.sha256(native.read_bytes()).hexdigest(), native_hash)
        self.assertEqual(len(calls), 2)

    def test_liquid_library_checksum_failure_prevents_extraction(self):
        (self.path / "libmpfr4_3.1.6-1_amd64.deb").write_bytes(b"incorrect package bytes")
        with patch.object(prepare, "execute") as execute:
            with self.assertRaisesRegex(ValueError, "checksum"):
                prepare.liquid_linux_libraries(self.path / "pro_sphwv", self.path)
        execute.assert_not_called()

    def test_worker_validation_is_small_serial_and_separate_from_campaign_records(self):
        runtime = dict(availability={"acousticts": {"status": "ready"}, "echosms": {"status": "ready"}})
        observed = []

        def measure(case, actual_runtime, directory, timeout):
            observed.append(case["software"])
            self.assertIs(actual_runtime, runtime)
            self.assertEqual((case["semimajor_m"], case["semiminor_m"], case["frequency_hz"]),
                             (.001, .0005, 1000))
            self.assertEqual((case["boundary"], case["incidence_deg"]), ("gas", 37))
            self.assertEqual(directory, self.path / "validation" / case["software"])
            self.assertLessEqual(timeout, 120)
            return dict(status="ok" if case["software"] == "acousticts" else "setup_error")

        with patch.object(prepare, "measure", side_effect=measure):
            self.assertFalse(prepare.validate_workers(["acousticts", "echosms"], runtime, self.path, 120))
        self.assertEqual(observed, ["acousticts", "echosms"])
        report = prepare.read_json(self.path / "validation.json")
        self.assertFalse(report["passed"])
        self.assertEqual([item["passed"] for item in report["workers"]], [True, False])
        self.assertFalse((self.path / "records").exists())
        self.assertFalse((self.path / "results.json").exists())

    def test_liquid_only_does_not_download_or_install_acoustic_scattering(self):
        def fake_execute(command, **kwargs):
            self.assertEqual(command[4], "-")
            project = Path(command[3])
            project.mkdir()
            for filename in ("Project.toml", "Manifest.toml", "LocalPreferences.toml"):
                (project / filename).write_text("fixture = true\n", encoding="utf-8")

        with patch.object(prepare, "obtain_source", side_effect=self.fake_source) as obtain, \
                patch.object(prepare, "execute", side_effect=fake_execute), \
                patch.object(prepare, "liquid_linux_libraries", return_value=("/fixture/libs", {"package": "fixture"})), \
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
