"""Select and verify identical Python and GNU versions before numerical builds."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import urllib.request

PYTHON_VERSION = "3.12.10"
GNU_VERSION = "14.3.0"
BREW_FORMULA_REVISION = "48ab05e29e7eae44525f5d162f2a2a87e7c19125"
BREW_BOTTLES = {
    ("Linux", "x86_64"): ("x86_64_linux", "fa7011beba62b377bb18d0c19583de797d5148250a4075714e14a85a387175ee"),
    ("Darwin", "arm64"): ("arm64_tahoe", "0684ab0865925ab2cde02c508212aaf063188b73a1d689e3b0be779a915b8d3b"),
}


def run(command, *, cwd=None, env=None):
    result = subprocess.run([str(value) for value in command], check=False,
                            capture_output=True, text=True, timeout=600, cwd=cwd, env=env)
    if result.returncode:
        output = (result.stdout + "\n" + result.stderr)[-12000:]
        raise RuntimeError(f"Toolchain command failed ({result.returncode}): {command[0]}\n{output}")
    return result.stdout.strip()


def download_bottle(path, digest):
    """Download the immutable public bottle and verify every byte before installation."""
    if path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == digest:
        return
    auth = "https://ghcr.io/token?service=ghcr.io&scope=repository:homebrew/core/gcc/14:pull"
    with urllib.request.urlopen(auth, timeout=30) as response:
        token = json.load(response)["token"]
    request = urllib.request.Request(
        "https://ghcr.io/v2/homebrew/core/gcc/14/blobs/sha256:" + digest,
        headers={"Authorization": "Bearer " + token})
    temporary = path.with_suffix(path.suffix + ".partial")
    checksum = hashlib.sha256()
    total = 0
    try:
        with urllib.request.urlopen(request, timeout=60) as response, temporary.open("wb") as output:
            while block := response.read(1024 * 1024):
                total += len(block)
                if total > 512 * 1024 * 1024:
                    raise RuntimeError("Pinned compiler bottle exceeds512MiB")
                checksum.update(block)
                output.write(block)
        if checksum.hexdigest() != digest:
            raise RuntimeError("Pinned compiler bottle SHA256 mismatch")
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def install_prefix(directory):
    """Use Rtools45 on Windows and verified historical Homebrew bottles elsewhere."""
    system, machine = platform.system(), platform.machine().lower()
    if system == "Windows":
        return Path(os.environ.get("RTOOLS45_HOME", "C:/rtools45")) / "x86_64-w64-mingw32.static.posix"
    bottle = BREW_BOTTLES.get((system, machine))
    if bottle is None:
        raise RuntimeError(f"No pinned GNU bottle for {system}/{machine}")
    brew = shutil.which("brew")
    if brew is None:
        fallback = Path("/opt/homebrew/bin/brew" if system == "Darwin" else "/home/linuxbrew/.linuxbrew/bin/brew")
        if not fallback.is_file():
            raise RuntimeError("The hosted runner must provide Homebrew")
        brew = str(fallback)
    os.environ.update(HOMEBREW_NO_AUTO_UPDATE="1", HOMEBREW_NO_INSTALL_CLEANUP="1",
                      HOMEBREW_NO_INSTALLED_DEPENDENTS_CHECK="1")
    prefix = Path(run([brew, "--prefix"])) / "opt/gcc@14"
    try:
        verify_compilers(compiler_paths(prefix, system))
        return prefix
    except (RuntimeError, subprocess.CalledProcessError):
        pass
    tag, digest = bottle
    archive = Path(directory) / f"gcc@14--{GNU_VERSION}.{tag}.bottle.4.tar.gz"
    archive.parent.mkdir(parents=True, exist_ok=True)
    download_bottle(archive, digest)
    if prefix.exists():
        run([brew, "uninstall", "--ignore-dependencies", "gcc@14"])
    # Homebrew 7 requires explicit opt-in for verified local bottles.
    # A bare filename also avoids interpreting an absolute path as a tap.
    run([brew, "install", "--force-bottle", archive.name], cwd=archive.parent,
        env=dict(os.environ, HOMEBREW_INTERNAL_ALLOW_PACKAGES_FROM_PATHS="1"))
    verify_compilers(compiler_paths(prefix, system))
    return prefix


def verify_python(version=None):
    actual = platform.python_version() if version is None else version
    if actual != PYTHON_VERSION:
        raise RuntimeError(f"Python {PYTHON_VERSION} is required, found {actual}")
    return actual


def compiler_paths(prefix, system):
    prefix = Path(prefix)
    suffix = ".exe" if system == "Windows" else "-14"
    return {name: prefix / "bin" / (executable + suffix)
            for name, executable in (("CC", "gcc"), ("CXX", "g++"), ("FC", "gfortran"))}


def verify_compilers(compilers):
    versions = {}
    for name, executable in compilers.items():
        if not executable.is_file():
            raise RuntimeError(f"Missing pinned {name} compiler: {executable}")
        version = run([executable, "-dumpfullversion", "-dumpversion"])
        if version != GNU_VERSION:
            raise RuntimeError(f"GNU {GNU_VERSION} is required for {name}, found {version}")
        versions[name] = version
        print(f"Verified {name}: {executable} ({version})", flush=True)
    return versions


def cache_identity(evidence):
    stable = {key: evidence[key] for key in
              ("python", "gnu", "system", "architecture", "compiler_sha256", "policy_sha256")}
    return hashlib.sha256(json.dumps(stable, sort_keys=True).encode()).hexdigest()[:24]


def r_makevars(compilers, *, system, directory):
    values = {key: str(path).replace("\\", "/") for key, path in compilers.items()}
    for path in values.values():
        if any(character in path for character in " \t\n\r#$`'\""):
            raise RuntimeError(f"Unsupported character in compiler path: {path}")
    values["F77"] = values["FC"]
    for standard in (11, 14, 17, 20, 23, 26):
        values[f"CXX{standard}"] = values["CXX"]
    if system == "Linux":
        libraries = []
        for library in ("libgfortran.so", "libquadmath.so"):
            filename = Path(run([compilers["FC"], f"-print-file-name={library}"]))
            if not filename.is_absolute() or not filename.is_file():
                raise RuntimeError(f"Pinned compiler cannot resolve {library}: {filename}")
            libraries.append(filename.resolve().parent)
        libraries = list(dict.fromkeys(libraries))
        values["FLIBS"] = " ".join(f"-L{path} -Wl,-rpath,{path}" for path in libraries)
        values["FLIBS"] += " -lgfortran -lquadmath -lm"
    path = Path(directory) / "pinned-gnu.Makevars"
    path.write_text("".join(f"{key} = {value}\n" for key, value in values.items()), encoding="utf-8")
    values["R_MAKEVARS_USER"] = path.as_posix()
    return values


def load_macos_setup():
    path = Path(os.environ.get("MACOS_SETUP_SCRIPT", str(Path(__file__).with_name("setup_macos.py"))))
    spec = importlib.util.spec_from_file_location("benchmark_macos_toolchain", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def quad_preflight(directory, compilers, values):
    """Compile and execute a mixed C++/Fortran binary128 ABI check."""
    probes = load_macos_setup()
    for name, source in (("pinned-quad.f90", probes.FORTRAN_PROBE),
                         ("pinned-quad.cpp", probes.CPP_PROBE)):
        (directory / name).write_text(source, encoding="utf-8")
    object_file = directory / "pinned-quad.o"
    executable = directory / ("pinned-quad.exe" if platform.system() == "Windows" else "pinned-quad")
    run([compilers["FC"], "-O2", "-c", directory / "pinned-quad.f90", "-o", object_file])
    libraries = values.get("FLIBS", "-lgfortran -lquadmath -lm").split()
    run([compilers["CXX"], "-std=gnu++17", "-O2", directory / "pinned-quad.cpp",
         object_file, *libraries, "-o", executable])
    run([executable])


def configure(directory, prefix=None, *, with_r=False):
    verify_python()
    prefix = install_prefix(directory) if prefix is None else prefix
    system = platform.system()
    compilers = compiler_paths(prefix, system)
    versions = verify_compilers(compilers)
    os.environ["PATH"] = str(Path(prefix) / "bin") + os.pathsep + os.environ.get("PATH", "")
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    if system == "Darwin":
        os.environ["BENCHMARK_GNU_PREFIX"] = str(prefix)
        os.environ["BENCHMARK_GNU_VERSION"] = GNU_VERSION
        macos = load_macos_setup()
        macos_evidence = macos.configure(directory, with_r=with_r)
        values = macos_evidence["environment"]
    else:
        values = r_makevars(compilers, system=system, directory=directory)
        quad_preflight(directory, compilers, values)
    values.update({f"FPM_{key}": str(path) for key, path in compilers.items()})
    values.update(BENCHMARK_GNU_VERSION=GNU_VERSION, BENCHMARK_PYTHON_VERSION=PYTHON_VERSION)
    evidence = dict(schema_version=1, python=PYTHON_VERSION, gnu=versions, system=system,
                    binary128_preflight=True,
                    architecture=platform.machine(),
                    compilers={key: str(path) for key, path in compilers.items()},
                    compiler_sha256={key: hashlib.sha256(path.read_bytes()).hexdigest()
                                     for key, path in compilers.items()},
                    policy_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    environment=values)
    if system != "Windows":
        tag, digest = BREW_BOTTLES[(system, platform.machine().lower())]
        evidence["distribution"] = dict(formula_revision=BREW_FORMULA_REVISION,
                                         bottle_tag=tag, bottle_sha256=digest)
    if system == "Darwin":
        evidence["macos"] = macos_evidence
        # Include the SDK and C++ ABI setup in the cross-platform cache identity.
        evidence["policy_sha256"] = hashlib.sha256((evidence["policy_sha256"] +
            values["MACOS_TOOLCHAIN_KEY"]).encode()).hexdigest()
    identity = cache_identity(evidence)
    evidence["cache_key"] = identity
    target = directory / "toolchain.json"
    target.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    values.update(BENCHMARK_TOOLCHAIN_KEY=identity, BENCHMARK_TOOLCHAIN_FILE=target.as_posix())
    os.environ.update(values)
    if os.environ.get("GITHUB_ENV"):
        with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as stream:
            for key, value in values.items():
                stream.write(f"{key}={value}\n")
    if os.environ.get("GITHUB_PATH"):
        with open(os.environ["GITHUB_PATH"], "a", encoding="utf-8") as stream:
            stream.write(str(Path(prefix) / "bin") + "\n")
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--prefix", type=Path)
    parser.add_argument("--with-r", action="store_true")
    args = parser.parse_args()
    configure(args.directory, args.prefix, with_r=args.with_r)


if __name__ == "__main__":
    main()
