"""Install pinned sources into a disposable runner workspace, outside timing."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import sys
import time
import tomllib
import urllib.parse
import urllib.request
import zipfile

from benchmark import ROOT, atomic_json, fingerprint, read_json


def compatibility_evidence(evidence, workspace):
    """Retain numerical build inputs while ignoring nondeterministic outputs."""
    raw_path = str(workspace).rstrip("/\\")
    slash_path = raw_path.replace("\\", "/")
    uri = "file:///" + slash_path.lstrip("/")
    variants = {raw_path, slash_path, raw_path.replace("\\", "\\\\"), uri,
                urllib.parse.quote(slash_path, safe="/:"),
                urllib.parse.quote(uri, safe="/:"),
                "file:///" + urllib.parse.quote(slash_path.lstrip("/"), safe="/")}

    def normalize(value):
        if isinstance(value, dict):
            return {key: normalize(item) for key, item in value.items()
                    if key not in ("prol_native_sha256", "r_installed_sha256", "archive_sha256")}
        if isinstance(value, list):
            return [normalize(item) for item in value]
        if isinstance(value, str):
            for variant in sorted(variants, key=len, reverse=True):
                value = value.replace(variant, "<workspace>")
        return value

    normalized = normalize(evidence)
    for environment in normalized.get("julia_environments", {}).values():
        # Pkg's derived project hash can change with an absolute development
        # path. The actual normalized project and dependency records remain.
        manifest = environment.get("Manifest.toml")
        if isinstance(manifest, str):
            parsed = tomllib.loads(manifest)
            parsed.pop("project_hash", None)
            environment["Manifest.toml"] = parsed
    return normalized


def remaining_budget():
    remaining = float(os.environ.get("BENCHMARK_DEADLINE", "inf")) - time.time()
    if remaining <= 0:
        raise TimeoutError("Campaign deadline reached during preparation")
    return remaining


def execute(command, *, workspace, cwd=None, env=None, capture_output=False,
            timeout_seconds=1500):
    """Run setup with the same process-tree ownership as timed workers."""
    timeout = min(timeout_seconds, remaining_budget())
    print("SETUP", " ".join(map(str, command)), flush=True)
    command = list(map(str, command))
    gate = Path(workspace) / ".setup-launch.ready"
    gate.unlink(missing_ok=True)
    job = None
    if os.name == "nt":
        from process_control import WindowsJob
        job = WindowsJob()
        command = [sys.executable, str(ROOT / "src/process_gate.py"), str(gate), *command]
    process = None
    assigned = False
    try:
        process = subprocess.Popen(command, cwd=cwd, env=env, text=True,
            stdout=subprocess.PIPE if capture_output else None,
            start_new_session=os.name != "nt",
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        if job:
            job.assign(process)
            assigned = True
            gate.touch()
        try:
            output, _ = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            if job:
                job.terminate()
            else:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            process.communicate(timeout=10)
            raise TimeoutError(f"Setup command exceeded {timeout:.1f} seconds") from None
        if process.returncode:
            raise subprocess.CalledProcessError(process.returncode, command, output=output)
        return output
    finally:
        if process is not None and process.poll() is None:
            if job and assigned:
                job.terminate()
            elif job:
                process.kill()  # The unopened gate has not launched children.
            else:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            process.wait(timeout=10)
        if job:
            job.close()
        elif process is not None:
            # A setup process must not leave a compiler child competing with
            # the next package installation or timed prediction.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        gate.unlink(missing_ok=True)


def obtain_source(name, source, directory):
    destination = directory / name
    marker = destination / ".benchmark-source.json"
    if marker.exists():
        recorded = read_json(marker)
        if recorded["sha"] != source["sha"]:
            raise ValueError(f"Existing {name} source differs from requested SHA")
        if not recorded.get("files_sha256"):
            raise ValueError(f"Existing {name} source lacks file verification evidence")
        for relative, expected in recorded["files_sha256"].items():
            path = destination / relative
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                raise ValueError(f"Existing {name} source was modified: {relative}")
        return destination
    if destination.exists():
        raise ValueError(f"Refusing to replace existing source directory: {destination}")
    archive = directory / f"{name}-{source['sha']}.zip"
    url = f"https://codeload.github.com/{source['repository']}/zip/{source['sha']}"
    with urllib.request.urlopen(url, timeout=min(60, remaining_budget())) as incoming, archive.open("wb") as outgoing:
        while chunk := incoming.read(1024 * 1024):
            remaining_budget()
            outgoing.write(chunk)
    file_hashes = {}
    with zipfile.ZipFile(archive) as bundle:
        members = bundle.infolist()
        prefix = members[0].filename.split("/")[0] + "/"
        for member in members:
            remaining_budget()
            if not member.filename.startswith(prefix):
                raise ValueError("Unexpected archive root")
            relative = Path(member.filename[len(prefix):])
            if relative.is_absolute() or ".." in relative.parts or ":" in str(relative):
                raise ValueError("Unsafe source archive path")
            if (member.external_attr >> 16) & 0o170000 == 0o120000:
                raise ValueError("Source archives containing symbolic links are not supported")
            target = destination / relative
            if member.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with bundle.open(member) as incoming, target.open("wb") as outgoing:
                    shutil.copyfileobj(incoming, outgoing)
                file_hashes[relative.as_posix()] = hashlib.sha256(target.read_bytes()).hexdigest()
    atomic_json(marker, dict(source, archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
                            files_sha256=file_hashes))
    return destination


def compile_prol(source, build):
    build.mkdir(parents=True, exist_ok=True)
    compiler = os.environ.get("FC", "gfortran")
    executable = build / ("profcn_II.exe" if os.name == "nt" else "profcn_II")
    execute([compiler, "-O2", "-c", source / "src/param.f90", "-o", build / "param.o"], cwd=build, workspace=build)
    execute([compiler, "-O2", "-I", build, source / "src/profcn_II.f90", "-o", executable], cwd=build, workspace=build)
    return executable


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--software", default="prol_spheroid,acousticts,acousticscattering,echosms,liquid_spheroid")
    parser.add_argument("--sources-lock", default=str(ROOT / "sources.lock.json"))
    args = parser.parse_args()
    workspace = Path(args.directory).resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    sources = read_json(args.sources_lock)["sources"]
    names = args.software.split(",")
    if set(names) - sources.keys():
        raise ValueError("Unknown software selection")
    runtime = dict(source_paths={}, source_shas={name: sources[name]["sha"] for name in names},
                   executables={}, native_executables={}, availability={}, capabilities={}, env={})
    lock_evidence = {}
    for name in names:
        try:
            path = obtain_source(name, sources[name], workspace)
            runtime["source_paths"][name] = str(path)
            lock_evidence[name] = read_json(path / ".benchmark-source.json")
        except Exception as error:
            runtime["availability"][name] = dict(status="setup_error", message=str(error))
    python_names = [name for name in names if name in ("prol_spheroid", "echosms")
                    and name not in runtime["availability"]]
    if python_names:
        try:
            environment = workspace / "python"
            execute([sys.executable, "-m", "venv", environment], workspace=workspace)
            python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
            # lpmn is required by the unchanged Prol_Spheroid numerical source.
            execute([python, "-m", "pip", "install", "numpy==2.2.6", "scipy==1.15.3",
                     "matplotlib==3.10.3", "pandas==2.3.0", "hatchling", "hatch-vcs"], workspace=workspace)
            for name in python_names:
                runtime["executables"][name] = [str(python)]
        except Exception as error:
            for name in python_names:
                runtime["availability"][name] = dict(status="setup_error", message=str(error))
        if "echosms" in python_names and "echosms" not in runtime["availability"]:
            try:
                env = os.environ.copy()
                env["SETUPTOOLS_SCM_PRETEND_VERSION_FOR_ECHOSMS"] = "0.0.0+g" + sources["echosms"]["sha"][:12]
                execute([python, "-m", "pip", "install", "--no-build-isolation",
                         "--constraint", ROOT / "src/python-constraints.txt",
                         runtime["source_paths"]["echosms"]], env=env, workspace=workspace)
            except Exception as error:
                runtime["availability"]["echosms"] = dict(status="setup_error", message=str(error))
        if any(name not in runtime["availability"] for name in python_names):
            try:
                freeze = execute([python, "-m", "pip", "freeze"], capture_output=True,
                                 workspace=workspace, timeout_seconds=60)
                (workspace / "python-freeze.txt").write_text(freeze, encoding="utf-8")
                lock_evidence["python_packages"] = freeze
            except Exception as error:
                for name in python_names:
                    runtime["availability"].setdefault(name, dict(status="setup_error", message=str(error)))
    if "prol_spheroid" in runtime["source_paths"] and "prol_spheroid" not in runtime["availability"]:
        try:
            runtime["native_executables"]["prol_spheroid"] = str(compile_prol(
                Path(runtime["source_paths"]["prol_spheroid"]), workspace / "prol-build"))
            lock_evidence["prol_compiler"] = execute(
                [os.environ.get("FC", "gfortran"), "--version"], capture_output=True,
                workspace=workspace, timeout_seconds=30)
            lock_evidence["prol_flags"] = ["-O2"]
            lock_evidence["prol_native_sha256"] = hashlib.sha256(
                Path(runtime["native_executables"]["prol_spheroid"]).read_bytes()).hexdigest()
        except Exception as error:
            runtime["availability"]["prol_spheroid"] = dict(status="setup_error", message=str(error))
    if "acousticts" in runtime["source_paths"]:
        try:
            library = workspace / "r-library"
            library.mkdir(exist_ok=True)
            env = os.environ.copy()
            env["R_LIBS_USER"] = str(library)
            execute(["Rscript", "--vanilla", ROOT / "src/setup_r.R",
                     library, runtime["source_paths"]["acousticts"], workspace / "r-packages.json"], env=env, workspace=workspace)
            runtime["env"]["R_LIBS_USER"] = str(library)
            runtime["executables"]["acousticts"] = ["Rscript", "--vanilla"]
            lock_evidence["r_packages"] = read_json(workspace / "r-packages.json")
            runtime["capabilities"]["acousticts"] = lock_evidence["r_packages"]["capabilities"]
            package = library / "acousticTS"
            lock_evidence["r_installed_sha256"] = {
                path.relative_to(package).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(package.rglob("*"))
                if path.is_file() and path.suffix.lower() in (".dll", ".so", ".dylib", ".rdb", ".rdx")}
        except Exception as error:
            runtime["availability"]["acousticts"] = dict(status="setup_error", message=str(error))
    julia_names = [n for n in names if n in ("acousticscattering", "liquid_spheroid")
                   and n in runtime["source_paths"]]
    for name in julia_names:
        if name == "liquid_spheroid" and (platform.system() not in ("Linux", "Windows") or
                platform.machine().lower() not in ("amd64", "x86_64")):
            runtime["availability"][name] = dict(status="unsupported", message="Upstream native executables target Linux/Windows x64")
            continue
        try:
            # Independent environments isolate heavy BEM dependencies from the
            # legacy solver and permit either installation to fail separately.
            project = workspace / ("julia" if name == "acousticscattering" else "julia-liquid_spheroid")
            command = ["julia", "--startup-file=no", ROOT / "src/setup_julia.jl", project,
                       runtime["source_paths"][name] if name == "acousticscattering" else "-"]
            if name == "liquid_spheroid":
                command.append(runtime["source_paths"][name])
            execute(command, workspace=workspace)
            runtime["executables"][name] = ["julia", "--startup-file=no", "--threads=1", f"--project={project}"]
            evidence = {}
            for filename in ("Project.toml", "Manifest.toml", "LocalPreferences.toml"):
                evidence[filename] = (project / filename).read_text()
            lock_evidence.setdefault("julia_environments", {})[name] = evidence
        except Exception as error:
            runtime["availability"][name] = dict(status="setup_error", message=str(error))
    if "liquid_spheroid" in julia_names and "liquid_spheroid" not in runtime["availability"]:
        if platform.system() not in ("Linux", "Windows") or platform.machine().lower() not in ("amd64", "x86_64"):
            runtime["availability"]["liquid_spheroid"] = dict(status="unsupported", message="Upstream native executables target Linux/Windows x64")
        else:
            folder = "windows" if os.name == "nt" else "linux"
            filename = "pro_sphwv.exe" if os.name == "nt" else "pro_sphwv"
            executable = Path(runtime["source_paths"]["liquid_spheroid"]) / folder / filename
            if not executable.is_file():
                runtime["availability"]["liquid_spheroid"] = dict(status="setup_error", message=f"Missing {executable}")
            else:
                executable.chmod(executable.stat().st_mode | 0o111)
                runtime["native_executables"]["liquid_spheroid"] = str(executable)
                lock_evidence["liquid_native_sha256"] = hashlib.sha256(executable.read_bytes()).hexdigest()
    for name in names:
        runtime["availability"].setdefault(name, dict(status="ready"))
    runtime["environment_evidence"] = lock_evidence
    runtime["environment_compatibility"] = compatibility_evidence(lock_evidence, workspace)
    runtime["environment_fingerprint"] = fingerprint(runtime["environment_compatibility"])
    atomic_json(workspace / "runtime.json", runtime)
    print(json.dumps(runtime["availability"], indent=2), flush=True)


if __name__ == "__main__":
    main()
