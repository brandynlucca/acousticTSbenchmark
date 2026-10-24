"""Configure native macOS GNU compilers without modifying numerical sources."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess


FORTRAN_PROBE = """subroutine benchmark_quad(value) bind(C)
  use, intrinsic :: iso_fortran_env, only: real128
  real(real128), intent(out) :: value
  if (digits(value) /= 113) stop 1
  value = sqrt(2.0_real128)
end subroutine benchmark_quad
"""

CPP_PROBE = """#include <quadmath.h>
#include <complex>
extern "C" void benchmark_quad(__float128*);
int main() {
    static_assert(FLT128_MANT_DIG == 113, "IEEE binary128 required");
    __float128 value;
    benchmark_quad(&value);
    const __float128 two = static_cast<__float128>(2);
    const std::complex<__float128> z(value, jnq(1, two));
    const std::complex<__float128> ratio = z / z;
    return !finiteq(value) || fabsq(value - sqrtq(two)) > 1e-32Q ||
        fabsq(ratio.real() - 1) > 1e-32Q || fabsq(ratio.imag()) > 1e-32Q;
}
"""

R_DEPENDENCIES = """args <- commandArgs(trailingOnly = TRUE)
library_path <- normalizePath(args[[1L]], mustWork = TRUE)
.libPaths(c(library_path, .libPaths()))
options(repos = c(CRAN = "https://cloud.r-project.org"), Ncpus = 1L)
# Rcpp exposes C++ objects across package boundaries. Its binary and its
# clients must use the same C++ standard library as acousticTS.
for (package in c("Rcpp", "RcppArmadillo")) {
  message("Building ", package, " from source with the macOS GNU toolchain")
  install.packages(package, lib = library_path, type = "source",
                   dependencies = FALSE)
  if (!requireNamespace(package, lib.loc = library_path, quietly = TRUE))
    stop("GNU source installation failed for ", package)
}
library(Rcpp, lib.loc = library_path)
Rcpp::sourceCpp(code = '
  #include <RcppArmadillo.h>
  // [[Rcpp::depends(RcppArmadillo)]]
  // [[Rcpp::export]]
  double benchmark_cpp_abi() {
    Rcpp::Rcout << "GNU Rcpp ABI probe" << std::endl;
    arma::mat x = arma::eye<arma::mat>(2, 2);
    return arma::accu(x);
  }', cacheDir = args[[2L]], rebuild = TRUE, verbose = FALSE)
stopifnot(identical(benchmark_cpp_abi(), 2))
cat("GNU Rcpp and RcppArmadillo ABI validation passed\\n")
"""


def run(command, *, env=None, cwd=None, timeout=60, log_path=None):
    """Capture setup diagnostics and terminate the process group on timeout."""
    command = list(map(str, command))
    print("MACOS SETUP", " ".join(command), flush=True)
    process = subprocess.Popen(command, cwd=cwd, env=env, text=True,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               start_new_session=os.name == "posix")
    try:
        output, _ = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        if os.name == "posix":
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        else:
            process.kill()
        output, _ = process.communicate()
        if log_path:
            Path(log_path).write_text(output, encoding="utf-8")
        raise RuntimeError(f"macOS setup command exceeded {timeout} seconds: {command[0]}\n{output[-12000:]}")
    if log_path:
        Path(log_path).write_text(output, encoding="utf-8")
    if process.returncode:
        raise RuntimeError(f"macOS setup command failed ({process.returncode}): {' '.join(command)}\n{output[-12000:]}")
    return output.strip()


def select_sdk(applications=Path("/Applications")):
    """Use an installed Xcode 26 SDK, never the newer Command Line Tools SDK."""
    candidates = []
    for app in applications.glob("Xcode_26*.app"):
        match = re.fullmatch(r"Xcode_(26(?:\.\d+){1,2})\.app", app.name)
        if match and (app / "Contents/Developer").is_dir():
            candidates.append((tuple(map(int, match[1].split("."))), app))
    if not candidates:
        raise RuntimeError("No supported Xcode 26 installation found. The macOS benchmark requires its SDK, not Command Line Tools 27.")
    developer = min(candidates)[1] / "Contents/Developer"
    env = dict(os.environ, DEVELOPER_DIR=str(developer))
    env.pop("SDKROOT", None)
    sdk = Path(run(["xcrun", "--sdk", "macosx", "--show-sdk-path"], env=env))
    version = run(["xcrun", "--sdk", "macosx", "--show-sdk-version"], env=env)
    if (not re.fullmatch(r"26(?:\.\d+){0,2}", version) or not sdk.is_dir() or
            not sdk.resolve().is_relative_to(developer.resolve())):
        raise RuntimeError(f"Unexpected Xcode SDK selection: {sdk} ({version})")
    return developer, sdk, version


def gnu_compilers():
    prefix = Path(run(["brew", "--prefix", "gcc@14"]))
    if not prefix.is_absolute():
        raise RuntimeError("Homebrew returned a non-absolute GCC 14 installation path")
    compilers = {name: prefix / "bin" / executable for name, executable in
                 (("FC", "gfortran-14"), ("CC", "gcc-14"), ("CXX", "g++-14"))}
    versions = {}
    for name, executable in compilers.items():
        if not executable.is_file():
            raise RuntimeError(f"Installed Homebrew GCC 14 compiler is missing: {executable}")
        version = run([executable, "-dumpfullversion", "-dumpversion"])
        if not re.fullmatch(r"14(?:\.\d+){1,2}", version):
            raise RuntimeError(f"Expected GCC 14 for {name}, found {version}")
        versions[name] = version
    if len(set(versions.values())) != 1:
        raise RuntimeError(f"C, C++, and Fortran compiler versions differ: {versions}")
    return compilers, versions


def build_environment(directory, compilers, developer, sdk):
    """Give configure and R's build system the same GNU compiler/runtime ABI."""
    libraries = []
    for library in ("libgfortran.dylib", "libquadmath.dylib"):
        path = Path(run([compilers["FC"], "-print-file-name=" + library]))
        if not path.is_absolute() or not path.is_file():
            raise RuntimeError(f"Homebrew GCC 14 runtime library is unavailable: {library} ({path})")
        if path.parent not in libraries:
            libraries.append(path.parent)
    # Runner paths contain no whitespace. Reject unexpected characters rather
    # than incorrectly quote shell commands embedded in R Makevars.
    for path in [*compilers.values(), developer, sdk, *libraries, directory]:
        if re.search(r"[\s#$`'\"\\]", path.as_posix()):
            raise RuntimeError(f"Unsupported characters in macOS toolchain path: {path}")
    flags = f"-g -O2 -Wall -fPIC -isysroot {sdk.as_posix()} -mmacosx-version-min=11.0"
    link_flags = f"-isysroot {sdk.as_posix()} -mmacosx-version-min=11.0"
    flibs = " ".join(f"-L{path.as_posix()} -Wl,-rpath,{path.as_posix()}" for path in libraries) + " -lgfortran -lquadmath -lm"
    values = {name: value.as_posix() for name, value in compilers.items()}
    values.update(DEVELOPER_DIR=developer.as_posix(), SDKROOT=sdk.as_posix(),
                  MACOSX_DEPLOYMENT_TARGET="11.0", CFLAGS=flags, FFLAGS=flags,
                  FCFLAGS=flags, FLIBS=flibs, LDFLAGS=link_flags)
    makevars = {key: values[key] for key in ("CC", "FC", "CFLAGS", "FFLAGS", "FCFLAGS", "FLIBS", "LDFLAGS")}
    makevars["F77"] = values["FC"]
    for standard in ("", "11", "14", "17", "20", "23"):
        key = "CXX" + standard
        values[key] = compilers["CXX"].as_posix()
        values[key + "FLAGS"] = flags
        makevars[key] = values[key]
        makevars[key + "FLAGS"] = flags
        if standard:
            values[key + "STD"] = "-std=gnu++" + standard
            makevars[key + "STD"] = values[key + "STD"]
    path = directory / "macos-gnu.Makevars"
    path.write_text("".join(f"{key} = {value}\n" for key, value in makevars.items()), encoding="utf-8")
    values["R_MAKEVARS_USER"] = path.as_posix()
    for name in ("FC", "CC", "CXX"):
        values["FPM_" + name] = values[name]
    return values


def quad_preflight(directory, values):
    env = dict(os.environ, **values)
    for name, content in (("macos-quad.f90", FORTRAN_PROBE), ("macos-quad.cpp", CPP_PROBE)):
        (directory / name).write_text(content, encoding="utf-8")
    common = ["-O2", "-isysroot", values["SDKROOT"], "-mmacosx-version-min=11.0"]
    run([values["FC"], *common, "-c", directory / "macos-quad.f90", "-o", directory / "macos-quad.o"], env=env)
    executable = directory / "macos-quad"
    run([values["CXX17"], "-std=gnu++17", *common, directory / "macos-quad.cpp",
         directory / "macos-quad.o", *values["FLIBS"].split(), "-o", executable], env=env)
    run([executable], env=env)
    print("Native GNU C++/Fortran IEEE binary128 preflight passed", flush=True)


def configure(directory, *, with_r=False):
    if platform.system() != "Darwin" or platform.machine().lower() not in ("arm64", "aarch64"):
        raise RuntimeError("This setup requires a native ARM64 macOS runner")
    directory.mkdir(parents=True, exist_ok=True)
    developer, sdk, sdk_version = select_sdk()
    compilers, versions = gnu_compilers()
    values = build_environment(directory, compilers, developer, sdk)
    quad_preflight(directory, values)
    evidence = dict(schema_version=1, architecture=platform.machine(), compilers=versions,
                    sdk=str(sdk), sdk_version=sdk_version, developer_dir=str(developer),
                    environment=values, with_r=with_r)
    # Absolute runner-temp paths change between jobs, so only stable toolchain
    # inputs enter the package cache identity.
    stable = dict(compilers=versions, sdk=str(sdk), sdk_version=sdk_version,
                  script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    key = hashlib.sha256(json.dumps(stable, sort_keys=True).encode()).hexdigest()[:20]
    values["MACOS_TOOLCHAIN_KEY"] = key
    (directory / "macos-toolchain.json").write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    if os.environ.get("GITHUB_ENV"):
        with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as output:
            for key, value in values.items():
                output.write(f"{key}={value}\n")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            output.write(f"cache_key={values['MACOS_TOOLCHAIN_KEY']}\n")
    return evidence


def rebuild_r(directory):
    evidence = json.loads((directory / "macos-toolchain.json").read_text(encoding="utf-8"))
    if not evidence["with_r"]:
        raise RuntimeError("Configure the macOS toolchain with --with-r before rebuilding R dependencies")
    library = directory / "r-library"
    library.mkdir(exist_ok=True)
    script = directory / "macos-r-dependencies.R"
    script.write_text(R_DEPENDENCIES, encoding="utf-8")
    env = dict(os.environ, **evidence["environment"], R_LIBS_USER=str(library))
    output = run(["Rscript", "--vanilla", script, library, directory], env=env,
                 timeout=420, log_path=directory / "setup-macos-r-dependencies.log")
    print(output[-12000:], flush=True)


def verify_r(directory):
    runtime = json.loads((directory / "runtime.json").read_text(encoding="utf-8"))
    capability = runtime.get("capabilities", {}).get("acousticts", {}).get("quad", {})
    if capability.get("available") is not True:
        raise RuntimeError("acousticTS native quad precision is unavailable after GNU setup: " + str(capability.get("message", "capability result missing")))
    validation = json.loads((directory / "validation.json").read_text(encoding="utf-8"))
    workers = [item for item in validation.get("workers", []) if item.get("software") == "acousticts"]
    if (len(workers) != 1 or workers[0].get("passed") is not True or
            workers[0].get("result", {}).get("status") != "ok"):
        raise RuntimeError("acousticTS quad worker validation did not pass after macOS GNU setup")
    print("acousticTS native quad capability and prediction validation passed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("configure", "rebuild-r", "verify-r"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--with-r", action="store_true")
    args = parser.parse_args()
    directory = args.directory.resolve()
    if args.stage == "configure":
        configure(directory, with_r=args.with_r)
    elif args.stage == "rebuild-r":
        rebuild_r(directory)
    else:
        verify_r(directory)


if __name__ == "__main__":
    main()
