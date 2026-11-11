"""Exact compiler selection, R build overrides, and immutable bottle validation."""

import copy
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import setup_toolchain


class ToolchainTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_python_patch_drift_is_rejected(self):
        self.assertEqual(setup_toolchain.verify_python("3.12.10"), "3.12.10")
        for version in ("3.12.15", "3.12.16", "3.13.0"):
            with self.assertRaisesRegex(RuntimeError, "Python3.12.10|Python 3.12.10"):
                setup_toolchain.verify_python(version)

    def test_all_compiler_languages_require_exact_version(self):
        compilers = setup_toolchain.compiler_paths(self.directory, "Linux")
        for compiler in compilers.values():
            compiler.parent.mkdir(exist_ok=True)
            compiler.touch()
        with patch.object(setup_toolchain, "run", side_effect=["14.3.0", "14.3.0", "14.4.0"]):
            with self.assertRaisesRegex(RuntimeError, "14.3.0.*FC.*14.4.0"):
                setup_toolchain.verify_compilers(compilers)
        with patch.object(setup_toolchain, "run", return_value="14.3.0") as run:
            self.assertEqual(setup_toolchain.verify_compilers(compilers),
                             {"CC": "14.3.0", "CXX": "14.3.0", "FC": "14.3.0"})
        self.assertTrue(all(call.args[0][-2:] == ["-dumpfullversion", "-dumpversion"]
                            for call in run.call_args_list))

    def test_r_makevars_overrides_configured_system_compilers(self):
        compilers = setup_toolchain.compiler_paths(self.directory, "Windows")
        values = setup_toolchain.r_makevars(compilers, system="Windows", directory=self.directory)
        text = Path(values["R_MAKEVARS_USER"]).read_text()
        for key in ("CC", "CXX", "CXX11", "CXX14", "CXX17", "CXX20", "CXX23", "CXX26", "FC", "F77"):
            self.assertIn(f"{key} = {values[key]}\n", text)
        self.assertEqual(values["CXX17"], compilers["CXX"].as_posix())
        self.assertEqual(values["FC"], compilers["FC"].as_posix())

    def test_linux_r_links_the_selected_fortran_runtime(self):
        libraries = [self.directory / name for name in ("libgfortran.so", "libquadmath.so")]
        for library in libraries:
            library.touch()
        compilers = setup_toolchain.compiler_paths(self.directory, "Linux")
        with patch.object(setup_toolchain, "run", side_effect=list(map(str, libraries))):
            values = setup_toolchain.r_makevars(compilers, system="Linux", directory=self.directory)
        self.assertIn(f"-L{self.directory}", values["FLIBS"])
        self.assertIn(f"-Wl,-rpath,{self.directory}", values["FLIBS"])

    def test_compiler_binary_or_policy_changes_invalidate_cache(self):
        base = dict(python="3.12.10", gnu={"CC": "14.3.0", "CXX": "14.3.0", "FC": "14.3.0"},
                    system="Linux", architecture="x86_64", compiler_sha256={"CXX": "first"},
                    policy_sha256="policy")
        for key, value in (("compiler_sha256", {"CXX": "second"}), ("python", "3.12.16"),
                           ("policy_sha256", "other-policy")):
            changed = copy.deepcopy(base)
            changed[key] = value
            self.assertNotEqual(setup_toolchain.cache_identity(base), setup_toolchain.cache_identity(changed))

    def test_bottle_checksum_failure_does_not_leave_installable_archive(self):
        target = self.directory / "gcc@14--14.3.0.x86_64_linux.bottle.4.tar.gz"
        replies = [io.BytesIO(json.dumps({"token": "public-token"}).encode()), io.BytesIO(b"corrupt")]
        with patch.object(setup_toolchain.urllib.request, "urlopen", side_effect=replies):
            with self.assertRaisesRegex(RuntimeError, "SHA256 mismatch"):
                setup_toolchain.download_bottle(target, "0" * 64)
        self.assertFalse(target.exists())
        self.assertFalse(target.with_suffix(target.suffix + ".partial").exists())

    def test_bottle_cache_is_reused_only_after_full_checksum(self):
        target = self.directory / "bottle.tar.gz"
        target.write_bytes(b"verified")
        with patch.object(setup_toolchain.urllib.request, "urlopen") as request:
            setup_toolchain.download_bottle(target, hashlib.sha256(b"verified").hexdigest())
        request.assert_not_called()

    def test_version_failure_cannot_export_a_usable_toolchain(self):
        environment = self.directory / "github-env"
        environment.write_text("EXISTING=retained\n")
        with patch.dict(os.environ, {"GITHUB_ENV": str(environment)}), \
                patch.object(setup_toolchain, "verify_python"), \
                patch.object(setup_toolchain, "verify_compilers", side_effect=RuntimeError("version mismatch")):
            with self.assertRaisesRegex(RuntimeError, "version mismatch"):
                setup_toolchain.configure(self.directory, self.directory)
        self.assertEqual(environment.read_text(), "EXISTING=retained\n")
        self.assertFalse((self.directory / "toolchain.json").exists())

    def test_local_bottle_installation_avoids_absolute_tap_parsing(self):
        with patch.object(setup_toolchain.platform, "system", return_value="Linux"), \
                patch.object(setup_toolchain.platform, "machine", return_value="x86_64"), \
                patch.object(setup_toolchain.shutil, "which", return_value="brew"), \
                patch.object(setup_toolchain, "verify_compilers", side_effect=[RuntimeError("absent"), {}]), \
                patch.object(setup_toolchain, "download_bottle"), \
                patch.object(setup_toolchain, "run", return_value=str(self.directory / "brew")) as command:
            setup_toolchain.install_prefix(self.directory)
        command.assert_any_call(["brew", "install", "--force-bottle",
            "gcc@14--14.3.0.x86_64_linux.bottle.4.tar.gz"], cwd=self.directory)


if __name__ == "__main__":
    unittest.main()
