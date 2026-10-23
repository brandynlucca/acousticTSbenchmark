"""Process-tree ownership and prediction-scoped resident-memory sampling."""

import ctypes
from ctypes import wintypes
import json
from pathlib import Path
import threading

try:
    import psutil
except (ImportError, OSError):
    psutil = None


MEMORY_INTERVAL_SECONDS = .01
MEMORY_METHOD = "prediction_incremental_process_tree_rss"


def memory_provenance():
    return dict(method=MEMORY_METHOD, sample_interval_seconds=MEMORY_INTERVAL_SECONDS,
                psutil_version=getattr(psutil, "__version__", None),
                scope="prediction only, additional summed worker and descendant RSS above post-warmup baseline",
                limitations="sampling can miss transient peaks, shared resident pages can be counted more than once")


class PredictionMemory:
    """Observe one prediction after an acknowledged post-warmup baseline."""

    def __init__(self, directory):
        self.paths = {name: Path(directory) / f"memory_{name}.json"
                      for name in ("ready", "ready_ack", "done", "done_ack", "failed")}
        self.stop = threading.Event()
        self.thread = None
        self.baseline = self.peak = None
        self.samples = 0
        self.completed = False
        self.error = None
        self.available = psutil is not None

    def request_fields(self):
        if not self.available:
            return {}
        try:
            for path in self.paths.values():
                path.unlink(missing_ok=True)
        except OSError as error:
            self.error = type(error).__name__
            self.available = False
            return {}
        return {f"memory_{name}_path": str(path) for name, path in self.paths.items()}

    def _release(self):
        if not self.available:
            return
        for name in ("ready_ack", "done_ack"):
            try:
                self.paths[name].touch()
            except OSError:
                pass

    def start(self, launched_pid):
        if not self.available:
            return
        try:
            self.thread = threading.Thread(target=self._run, args=(launched_pid,),
                                           name="prediction-memory", daemon=True)
            self.thread.start()
        except Exception as error:
            self.error = type(error).__name__
            self._release()

    def _rss(self, worker):
        if not worker.is_running():
            raise psutil.NoSuchProcess(worker.pid)
        total = worker.memory_info().rss
        if total <= 0:
            raise ValueError("Worker resident memory is unavailable")
        for child in worker.children(recursive=True):
            try:
                if child.is_running():
                    total += child.memory_info().rss
            except psutil.NoSuchProcess:
                # Exiting between enumeration and sampling is expected.
                continue
        return total

    def _run(self, launched_pid):
        try:
            launched = psutil.Process(launched_pid)
            worker = None
            while not self.stop.is_set():
                if self.paths["failed"].exists():
                    raise RuntimeError("Worker memory handshake failed")
                if worker is None and self.paths["ready"].exists():
                    try:
                        ready = json.loads(self.paths["ready"].read_text(encoding="utf-8"))
                    except json.JSONDecodeError:
                        self.stop.wait(MEMORY_INTERVAL_SECONDS)
                        continue
                    worker = psutil.Process(int(ready["pid"]))
                    if worker != launched and launched not in worker.parents():
                        raise ValueError("Memory worker is outside the owned process tree")
                    self.baseline = self.peak = self._rss(worker)
                    self.paths["ready_ack"].touch()
                elif worker is not None:
                    # The end handshake keeps the result alive for this final
                    # sample even when the prediction lasts less than 10 ms.
                    finished = self.paths["done"].exists()
                    self.peak = max(self.peak, self._rss(worker))
                    self.samples += 1
                    if finished:
                        self.completed = True
                        self.paths["done_ack"].touch()
                        return
                self.stop.wait(MEMORY_INTERVAL_SECONDS)
        except Exception as error:
            self.error = type(error).__name__
        finally:
            self._release()

    def finish(self, censored=False):
        self.stop.set()
        if self.thread is not None and self.thread.ident is not None:
            self.thread.join(timeout=1)
            if self.thread.is_alive():
                self.error = "SamplerShutdownTimeout"
        self._release()
        status = ("unavailable" if not self.available else "sampling_error" if self.error else
                  "not_started" if self.baseline is None else
                  "ok" if self.completed else "censored" if censored and self.samples else "incomplete")
        valid = status in ("ok", "censored")
        return dict(memory_method=MEMORY_METHOD, memory_status=status,
                    memory_delta_peak_bytes=max(0, self.peak - self.baseline) if valid else None,
                    memory_baseline_rss_bytes=self.baseline if valid else None,
                    memory_peak_rss_bytes=self.peak if valid else None,
                    memory_sample_interval_seconds=MEMORY_INTERVAL_SECONDS, memory_samples=self.samples,
                    memory_error=self.error, memory_psutil_version=getattr(psutil, "__version__", None))


class BasicLimits(ctypes.Structure):
    _fields_ = [("PerProcessUserTimeLimit", ctypes.c_int64),
                ("PerJobUserTimeLimit", ctypes.c_int64), ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t), ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD), ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD), ("SchedulingClass", wintypes.DWORD)]


class IOCounters(ctypes.Structure):
    _fields_ = [(name, ctypes.c_uint64) for name in (
        "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
        "ReadTransferCount", "WriteTransferCount", "OtherTransferCount")]


class ExtendedLimits(ctypes.Structure):
    _fields_ = [("BasicLimitInformation", BasicLimits), ("IoInfo", IOCounters),
                ("ProcessMemoryLimit", ctypes.c_size_t), ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t), ("PeakJobMemoryUsed", ctypes.c_size_t)]


class WindowsJob:
    def __init__(self):
        self.api = ctypes.WinDLL("kernel32", use_last_error=True)
        self.api.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        self.api.CreateJobObjectW.restype = wintypes.HANDLE
        self.api.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
        self.api.SetInformationJobObject.restype = wintypes.BOOL
        self.api.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        self.api.AssignProcessToJobObject.restype = wintypes.BOOL
        self.api.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
        self.api.TerminateJobObject.restype = wintypes.BOOL
        self.api.CloseHandle.argtypes = [wintypes.HANDLE]
        self.api.CloseHandle.restype = wintypes.BOOL
        self.handle = self.api.CreateJobObjectW(None, None)
        if not self.handle:
            raise ctypes.WinError(ctypes.get_last_error())
        limits = ExtendedLimits()
        limits.BasicLimitInformation.LimitFlags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not self.api.SetInformationJobObject(self.handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
            error = ctypes.WinError(ctypes.get_last_error())
            self.close()
            raise error

    def assign(self, process):
        if not self.api.AssignProcessToJobObject(self.handle, int(process._handle)):
            raise ctypes.WinError(ctypes.get_last_error())

    def terminate(self):
        if not self.api.TerminateJobObject(self.handle, 1):
            raise ctypes.WinError(ctypes.get_last_error())

    def close(self):
        if self.handle:
            self.api.CloseHandle(self.handle)
            self.handle = None
