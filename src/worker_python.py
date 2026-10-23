"""Public echoSMs and vectorized Prol_Spheroid single-prediction adapters."""

from __future__ import annotations

import builtins
import csv
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
import traceback
from types import SimpleNamespace


def memory_boundary(case, phase):
    """Acknowledge memory samples outside the public prediction timer."""
    signal = case.get(f"memory_{phase}_path")
    if not signal:
        return
    try:
        if phase == "ready":
            Path(signal).write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
        else:
            Path(signal).touch()
        acknowledgement = Path(case[f"memory_{phase}_ack_path"])
        deadline = time.monotonic() + 5
        while not acknowledgement.exists():
            if time.monotonic() >= deadline:
                raise TimeoutError("Memory sampler did not acknowledge the prediction boundary")
            time.sleep(.01)
    except Exception:
        try:
            Path(case["memory_failed_path"]).touch()
        except Exception:
            pass


class NoPlot:
    """Disable upstream plotting without changing numerical operations."""

    def __getattr__(self, name):
        return lambda *args, **kwargs: None


def prepare_echosms(case):
    import echosms
    from echosms.utils import boundary_type

    mapping = dict(rigid="fixed_rigid", pressure_release="pressure_release",
                   liquid="fluid_filled", gas="fluid_filled")

    def solve(problem):
        model = echosms.PSMSModel()
        kwargs = dict(medium_c=problem["medium_sound_speed_m_s"],
                      medium_rho=problem["medium_density_kg_m3"],
                      a=problem["semimajor_m"], b=problem["semiminor_m"],
                      theta=problem["incidence_deg"], f=problem["frequency_hz"],
                      boundary_type=getattr(boundary_type, mapping[problem["boundary"]]))
        if problem["boundary"] in ("liquid", "gas"):
            kwargs.update(target_c=problem["target_sound_speed_m_s"],
                          target_rho=problem["target_density_kg_m3"])
        ts = model.calculate_ts_single(**kwargs)
        return dict(ts_db=float(ts), diagnostics={"support": "public default modal limits",
                    "convergence_checked": False, "package_version": getattr(echosms, "__version__", None)})

    return solve


def prepare_prol(case):
    source = Path(case["source_path"]).resolve() / "src"
    sys.path.insert(0, str(source))
    import prol_spheroid_vectorized as model
    from IterativeSolvers import IterativeRefinement

    executable = Path(case["native_executable"]).resolve()
    if not executable.is_file():
        raise FileNotFoundError(f"Compiled Prol_Spheroid executable missing: {executable}")
    work = Path(case["work_directory"]).resolve()
    original_run = subprocess.run

    def run_native(command, *args, **kwargs):
        if isinstance(command, (list, tuple)):
            names = [Path(str(part)).name for part in command]
            if "Run_param_fort_FromPython.py" in names:
                return subprocess.CompletedProcess(command, 0, "", "")
            if "Run_profcn_II_fort_FromPython.py" in names:
                return original_run([str(executable)], cwd=work, check=True,
                                    capture_output=True, text=True)
        return original_run(command, *args, **kwargs)

    def local_open(path, *args, **kwargs):
        filename = Path(path).name
        if filename in ("profcn.dat", "fort.20", "fort.70"):
            path = work / filename
        return builtins.open(path, *args, **kwargs)

    # Only redirect launch, temporary files and plotting. Numerical source is unchanged.
    model.subprocess = SimpleNamespace(run=run_native)
    model.open = local_open
    model.plt = NoPlot()

    def solve(problem):
        for filename in ("profcn.dat", "fort.20", "fort.70", "prediction.csv"):
            (work / filename).unlink(missing_ok=True)
        controls = problem["controls"]
        frequency = problem["frequency_hz"]
        settings = SimpleNamespace(prefix=problem["boundary"],
            ro_w=problem["medium_density_kg_m3"], ro_s=problem["target_density_kg_m3"],
            c_w=problem["medium_sound_speed_m_s"], c_s=problem["target_sound_speed_m_s"],
            a=problem["semimajor_m"], b=problem["semiminor_m"],
            theta_i_deg=problem["incidence_deg"], min_freq=frequency, max_freq=frequency + 1,
            delta_f=1, precision_fbs=controls[f"{problem['boundary']}_precision_fbs"])
        solver = IterativeRefinement("LU", tol=controls["linear_tolerance"],
                                     maxiter=controls["linear_maxiter"], verbose=False)
        model.ProlateSpheroid(settings, solver).run(str(work / "prediction.csv"))
        with (work / "prediction.csv").open(encoding="utf-8") as stream:
            values = list(csv.DictReader(stream))
        if len(values) != 1 or abs(float(values[0]["Freq_kHz"]) * 1000 - frequency) > 0.01:
            raise ValueError("Unexpected Prol_Spheroid frequency output")
        return dict(ts_db=float(values[0]["TS"]), diagnostics={
            "implementation": "vectorized", "convergence_checked": False,
            "native_generation_included": True, "native_compilation_included": False,
            "public_csv_io_included": True, "plotting_disabled": True})

    return solve


def main():
    case = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8-sig"))
    output = Path(sys.argv[2])
    result = dict(status="setup_error", elapsed_seconds=None, ts_db=None,
                  amplitude_real_m=None, amplitude_imag_m=None,
                  runtime_version=platform.python_version())
    started = None
    try:
        solve = prepare_prol(case) if case["software"] == "prol_spheroid" else prepare_echosms(case)
        import numpy
        import scipy
        result["versions"] = dict(numpy=numpy.__version__, scipy=scipy.__version__)
        warmup_seconds = 0.0
        if case.get("warmup", True):
            warm = dict(case, semimajor_m=0.001, semiminor_m=0.0005, frequency_hz=1000)
            before = time.perf_counter()
            solve(warm)
            warmup_seconds = time.perf_counter() - before
        result["warmup_seconds"] = warmup_seconds
        memory_boundary(case, "ready")
        Path(case["timing_started_path"]).write_text(json.dumps({"started_epoch": time.time()}))
        started = time.perf_counter()
        prediction = solve(case)
        result["elapsed_seconds"] = time.perf_counter() - started
        memory_boundary(case, "done")
        result.update(prediction)
        if not math.isfinite(result["ts_db"]):
            result.update(status="numerical_error", ts_db=None, message="Nonfinite public TS")
        else:
            result["status"] = "ok"
    except Exception as error:
        if started is not None:
            if result["elapsed_seconds"] is None:
                result["elapsed_seconds"] = time.perf_counter() - started
            memory_boundary(case, "done")
            result["status"] = "numerical_error"
        traceback.print_exc()
        result["message"] = f"{type(error).__name__}: {error}"
    output.write_text(json.dumps(result, allow_nan=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
