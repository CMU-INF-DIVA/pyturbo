"""GPU resource allocator integrating Monitor and Registry."""

from __future__ import annotations

import logging
from typing import Optional

from .monitor import NvidiaMonitor
from .registry import FileLockRegistry

logger = logging.getLogger(__name__)


class ResourceAllocator:
    """Allocate GPUs using double-check: registry free AND monitor free.

    Implements ghost protection: if registry says free but monitor says occupied,
    the GPU is marked unavailable.
    """

    def __init__(self, monitor: NvidiaMonitor, registry: FileLockRegistry):
        self._monitor = monitor
        self._registry = registry

    def get_available_gpus(self) -> list[int]:
        """Return GPU IDs that are both registry-free and physically idle.

        Steps:
        1. Query registry for occupied GPUs (with stale cleanup)
        2. Query monitor for physical GPU states
        3. Detect ghosts (physically occupied but not in registry)
        4. Return intersection of registry-free and monitor-idle
        """
        registry_occupied = self._registry.get_occupied_gpus()
        monitor_idle = set(self._monitor.get_idle_gpu_ids())
        ghost_ids = set(self._monitor.detect_ghosts(registry_occupied))

        all_gpu_ids = set(range(self._monitor.get_gpu_count()))
        available = (all_gpu_ids - registry_occupied - ghost_ids) & monitor_idle

        available_sorted = sorted(available)
        logger.debug(
            "[ALLOCATOR] registry_occupied=%s, monitor_idle=%s, ghosts=%s, available=%s",
            registry_occupied,
            monitor_idle,
            ghost_ids,
            available_sorted,
        )
        return available_sorted

    def try_acquire(self, gpu_required: int, preferred_ids: Optional[list[int]] = None) -> Optional[list[int]]:
        """Try to acquire gpu_required GPUs. Returns GPU IDs or None if insufficient.

        If preferred_ids is given, try those first. Otherwise pick the first
        available contiguous block (minimize fragmentation).
        """
        available = self.get_available_gpus()

        if preferred_ids is not None:
            if all(gid in available for gid in preferred_ids) and len(preferred_ids) >= gpu_required:
                selected = preferred_ids[:gpu_required]
                self._registry.register(
                    job_id="__pending__", gpu_ids=selected
                )
                return selected

        if len(available) < gpu_required:
            logger.debug(
                "[ALLOCATOR] Not enough GPUs: need %d, available %d",
                gpu_required,
                len(available),
            )
            return None

        # Pick first N available (already sorted)
        selected = available[:gpu_required]
        return selected

    def acquire(self, job_id: str, gpu_ids: list[int], command: str = "") -> None:
        """Register the job as occupying the given GPUs."""
        self._registry.register(job_id=job_id, gpu_ids=gpu_ids, command=command)

    def release(self, job_id: str) -> None:
        """Release GPUs held by a job."""
        self._registry.release(job_id=job_id)
