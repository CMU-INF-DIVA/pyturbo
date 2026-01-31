"""Tests for ResourceAllocator."""

from unittest.mock import MagicMock

import pytest

from pyturbo.scheduler.allocator import ResourceAllocator
from pyturbo.scheduler.dataclasses import GPUState


@pytest.fixture
def mock_monitor():
    monitor = MagicMock()
    monitor.get_gpu_count.return_value = 8

    # GPUs 0-3 idle, 4-7 busy
    def get_states():
        states = []
        for i in range(8):
            states.append(
                GPUState(
                    id=i,
                    mem_used_mb=50 if i < 4 else 8000,
                    mem_total_mb=24000,
                    utilization_pct=0 if i < 4 else 95,
                    process_count=0 if i < 4 else 1,
                    available=i < 4,
                )
            )
        return states

    monitor.get_gpu_states.return_value = get_states()
    monitor.get_idle_gpu_ids.return_value = [0, 1, 2, 3]
    monitor.detect_ghosts.return_value = []
    return monitor


@pytest.fixture
def mock_registry():
    registry = MagicMock()
    registry.get_occupied_gpus.return_value = set()
    return registry


class TestResourceAllocator:
    def test_get_available_all_free(self, mock_monitor, mock_registry):
        allocator = ResourceAllocator(mock_monitor, mock_registry)
        available = allocator.get_available_gpus()
        assert available == [0, 1, 2, 3]

    def test_get_available_some_registered(self, mock_monitor, mock_registry):
        mock_registry.get_occupied_gpus.return_value = {0, 1}
        allocator = ResourceAllocator(mock_monitor, mock_registry)
        available = allocator.get_available_gpus()
        assert available == [2, 3]

    def test_get_available_with_ghosts(self, mock_monitor, mock_registry):
        mock_monitor.detect_ghosts.return_value = [2]
        allocator = ResourceAllocator(mock_monitor, mock_registry)
        available = allocator.get_available_gpus()
        assert available == [0, 1, 3]

    def test_try_acquire_success(self, mock_monitor, mock_registry):
        allocator = ResourceAllocator(mock_monitor, mock_registry)
        gpu_ids = allocator.try_acquire(4)
        assert gpu_ids == [0, 1, 2, 3]

    def test_try_acquire_insufficient(self, mock_monitor, mock_registry):
        allocator = ResourceAllocator(mock_monitor, mock_registry)
        gpu_ids = allocator.try_acquire(8)  # Only 4 available
        assert gpu_ids is None

    def test_try_acquire_partial(self, mock_monitor, mock_registry):
        allocator = ResourceAllocator(mock_monitor, mock_registry)
        gpu_ids = allocator.try_acquire(2)
        assert gpu_ids == [0, 1]

    def test_acquire_and_release(self, mock_monitor, mock_registry):
        allocator = ResourceAllocator(mock_monitor, mock_registry)
        allocator.acquire("T1-train", [0, 1, 2, 3], "echo test")
        mock_registry.register.assert_called_once_with(
            job_id="T1-train", gpu_ids=[0, 1, 2, 3], command="echo test"
        )

        allocator.release("T1-train")
        mock_registry.release.assert_called_once_with(job_id="T1-train")
