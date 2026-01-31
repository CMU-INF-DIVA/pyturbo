"""GPU monitoring via pynvml and inotify-based registry watcher."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional

from .dataclasses import GPUState

logger = logging.getLogger(__name__)

# Idle thresholds (§12.3)
MEM_IDLE_THRESHOLD_MB = 200
UTIL_IDLE_THRESHOLD_PCT = 5
PROCESS_COUNT_IDLE = 0


class NvidiaMonitor:
    """GPU state monitor using pynvml.

    Provides idle detection and ghost process detection.
    """

    def __init__(self):
        self._initialized = False

    def _ensure_init(self) -> None:
        if self._initialized:
            return
        try:
            import pynvml

            pynvml.nvmlInit()
            self._initialized = True
        except Exception as e:
            logger.warning("[MONITOR] pynvml initialization failed: %s", e)
            raise

    def get_gpu_count(self) -> int:
        self._ensure_init()
        import pynvml

        return pynvml.nvmlDeviceGetCount()

    def get_gpu_states(self) -> list[GPUState]:
        """Query all GPUs and return their states."""
        self._ensure_init()
        import pynvml

        count = pynvml.nvmlDeviceGetCount()
        states = []

        for i in range(count):
            handle = pynvml.nvmlDeviceGetHandleByIndex(i)
            mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
            util = pynvml.nvmlDeviceGetUtilizationRates(handle)

            try:
                procs = pynvml.nvmlDeviceGetComputeRunningProcesses(handle)
                proc_count = len(procs)
            except pynvml.NVMLError:
                proc_count = 0

            mem_used_mb = mem_info.used / (1024 * 1024)
            mem_total_mb = mem_info.total / (1024 * 1024)

            is_idle = (
                mem_used_mb < MEM_IDLE_THRESHOLD_MB
                and util.gpu < UTIL_IDLE_THRESHOLD_PCT
                and proc_count == PROCESS_COUNT_IDLE
            )

            states.append(
                GPUState(
                    id=i,
                    mem_used_mb=round(mem_used_mb, 1),
                    mem_total_mb=round(mem_total_mb, 1),
                    utilization_pct=util.gpu,
                    process_count=proc_count,
                    available=is_idle,
                )
            )

        return states

    def get_idle_gpu_ids(self) -> list[int]:
        """Return list of GPU IDs that are physically idle."""
        return [g.id for g in self.get_gpu_states() if g.available]

    def detect_ghosts(self, registry_occupied: set[int]) -> list[int]:
        """Detect GPUs that are physically occupied but not in registry.

        Returns list of ghost-occupied GPU IDs.
        """
        ghost_ids = []
        for state in self.get_gpu_states():
            if state.id not in registry_occupied and not state.available:
                logger.warning(
                    "[GHOST] GPU %d physically occupied but not in registry "
                    "(mem=%.0fMB, procs=%d)",
                    state.id,
                    state.mem_used_mb,
                    state.process_count,
                )
                ghost_ids.append(state.id)
        return ghost_ids


class RegistryWatcher:
    """Watch registry.json for changes using inotify (Linux only).

    Falls back to time.sleep() on non-Linux platforms.
    """

    def __init__(self, registry_path: str):
        self._registry_path = Path(registry_path)
        self._inotify = None

        try:
            import inotify.adapters

            self._inotify = inotify.adapters.Inotify()
            self._inotify.add_watch(str(self._registry_path.parent))
            self._registry_name = self._registry_path.name
            logger.info(
                "[WATCHER] inotify enabled for %s", self._registry_path.parent
            )
        except ImportError:
            logger.info(
                "[WATCHER] inotify not available, falling back to polling"
            )
        except Exception as e:
            logger.warning(
                "[WATCHER] inotify setup failed (%s), falling back to polling", e
            )

    def wait_for_change(self, timeout_s: int) -> bool:
        """Block until registry file changes or timeout.

        Returns True if a change was detected, False on timeout.
        """
        if self._inotify is None:
            time.sleep(timeout_s)
            return False

        for event in self._inotify.event_gen(timeout_s=timeout_s):
            if event is None:
                return False  # timeout
            _, type_names, _, filename = event
            if filename == self._registry_name and "IN_MODIFY" in type_names:
                return True  # registry changed
        return False
