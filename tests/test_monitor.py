"""Tests for NvidiaMonitor and RegistryWatcher (using mocks)."""

from unittest.mock import MagicMock, patch

import pytest

from pyturbo.scheduler.dataclasses import GPUState
from pyturbo.scheduler.monitor import (
    MEM_IDLE_THRESHOLD_MB,
    PROCESS_COUNT_IDLE,
    UTIL_IDLE_THRESHOLD_PCT,
    NvidiaMonitor,
    RegistryWatcher,
)


def make_mock_pynvml(gpu_configs):
    """Create a mock pynvml module with the given GPU configurations.

    gpu_configs: list of dicts with keys: mem_used, mem_total, util_gpu, proc_count
    """
    mock_pynvml = MagicMock()
    mock_pynvml.nvmlDeviceGetCount.return_value = len(gpu_configs)

    handles = [MagicMock() for _ in gpu_configs]
    mock_pynvml.nvmlDeviceGetHandleByIndex.side_effect = lambda i: handles[i]

    for i, cfg in enumerate(gpu_configs):
        mem_info = MagicMock()
        mem_info.used = cfg["mem_used"] * 1024 * 1024
        mem_info.total = cfg["mem_total"] * 1024 * 1024
        mock_pynvml.nvmlDeviceGetMemoryInfo.side_effect = (
            lambda h, configs=gpu_configs, handles=handles: _make_mem(
                configs[handles.index(h)]
            )
        )

        util = MagicMock()
        util.gpu = cfg["util_gpu"]
        mock_pynvml.nvmlDeviceGetUtilizationRates.side_effect = (
            lambda h, configs=gpu_configs, handles=handles: _make_util(
                configs[handles.index(h)]
            )
        )

        procs = [MagicMock() for _ in range(cfg["proc_count"])]
        mock_pynvml.nvmlDeviceGetComputeRunningProcesses.side_effect = (
            lambda h, configs=gpu_configs, handles=handles: [
                MagicMock() for _ in range(configs[handles.index(h)]["proc_count"])
            ]
        )

    # Override per-handle calls
    def get_mem(h):
        idx = handles.index(h)
        m = MagicMock()
        m.used = gpu_configs[idx]["mem_used"] * 1024 * 1024
        m.total = gpu_configs[idx]["mem_total"] * 1024 * 1024
        return m

    def get_util(h):
        idx = handles.index(h)
        u = MagicMock()
        u.gpu = gpu_configs[idx]["util_gpu"]
        return u

    def get_procs(h):
        idx = handles.index(h)
        return [MagicMock() for _ in range(gpu_configs[idx]["proc_count"])]

    mock_pynvml.nvmlDeviceGetMemoryInfo.side_effect = get_mem
    mock_pynvml.nvmlDeviceGetUtilizationRates.side_effect = get_util
    mock_pynvml.nvmlDeviceGetComputeRunningProcesses.side_effect = get_procs
    mock_pynvml.NVMLError = Exception

    return mock_pynvml


def _make_mem(cfg):
    m = MagicMock()
    m.used = cfg["mem_used"] * 1024 * 1024
    m.total = cfg["mem_total"] * 1024 * 1024
    return m


def _make_util(cfg):
    u = MagicMock()
    u.gpu = cfg["util_gpu"]
    return u


class TestNvidiaMonitor:
    def test_all_idle(self):
        configs = [
            {"mem_used": 50, "mem_total": 24000, "util_gpu": 0, "proc_count": 0},
            {"mem_used": 30, "mem_total": 24000, "util_gpu": 0, "proc_count": 0},
        ]
        mock = make_mock_pynvml(configs)

        with patch.dict("sys.modules", {"pynvml": mock}):
            monitor = NvidiaMonitor()
            monitor._initialized = True
            states = monitor.get_gpu_states()

        assert len(states) == 2
        assert all(s.available for s in states)

    def test_one_busy(self):
        configs = [
            {"mem_used": 50, "mem_total": 24000, "util_gpu": 0, "proc_count": 0},
            {"mem_used": 8000, "mem_total": 24000, "util_gpu": 95, "proc_count": 1},
        ]
        mock = make_mock_pynvml(configs)

        with patch.dict("sys.modules", {"pynvml": mock}):
            monitor = NvidiaMonitor()
            monitor._initialized = True
            states = monitor.get_gpu_states()

        assert states[0].available is True
        assert states[1].available is False

    def test_ghost_detection(self):
        configs = [
            {"mem_used": 5000, "mem_total": 24000, "util_gpu": 80, "proc_count": 1},
        ]
        mock = make_mock_pynvml(configs)

        with patch.dict("sys.modules", {"pynvml": mock}):
            monitor = NvidiaMonitor()
            monitor._initialized = True
            ghosts = monitor.detect_ghosts(registry_occupied=set())

        assert ghosts == [0]

    def test_no_ghost_when_registered(self):
        configs = [
            {"mem_used": 5000, "mem_total": 24000, "util_gpu": 80, "proc_count": 1},
        ]
        mock = make_mock_pynvml(configs)

        with patch.dict("sys.modules", {"pynvml": mock}):
            monitor = NvidiaMonitor()
            monitor._initialized = True
            ghosts = monitor.detect_ghosts(registry_occupied={0})

        assert ghosts == []

    def test_get_idle_gpu_ids(self):
        configs = [
            {"mem_used": 50, "mem_total": 24000, "util_gpu": 0, "proc_count": 0},
            {"mem_used": 8000, "mem_total": 24000, "util_gpu": 95, "proc_count": 1},
            {"mem_used": 30, "mem_total": 24000, "util_gpu": 2, "proc_count": 0},
        ]
        mock = make_mock_pynvml(configs)

        with patch.dict("sys.modules", {"pynvml": mock}):
            monitor = NvidiaMonitor()
            monitor._initialized = True
            idle = monitor.get_idle_gpu_ids()

        assert idle == [0, 2]


class TestRegistryWatcher:
    def test_fallback_when_no_inotify(self):
        """Should fall back to sleep when inotify is not available."""
        with patch.dict("sys.modules", {"inotify": None, "inotify.adapters": None}):
            watcher = RegistryWatcher("/tmp/test_registry.json")
            assert watcher._inotify is None

    def test_wait_timeout_without_inotify(self):
        """Without inotify, wait_for_change should just sleep and return False."""
        watcher = RegistryWatcher("/tmp/test_registry.json")
        watcher._inotify = None  # Force fallback
        import time
        start = time.time()
        result = watcher.wait_for_change(timeout_s=1)
        elapsed = time.time() - start
        assert result is False
        assert elapsed >= 0.9  # Should have slept ~1s
