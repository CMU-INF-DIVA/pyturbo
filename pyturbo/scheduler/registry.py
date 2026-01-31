"""FileLock-based GPU registry for concurrent job management."""

from __future__ import annotations

import fcntl
import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional

from .dataclasses import RegistryData, RegistryEntry

logger = logging.getLogger(__name__)


class FileLockRegistry:
    """GPU job registry using flock for mutual exclusion.

    Registry file: JSON containing active jobs and their GPU allocations.
    Lock file: Sibling file used for flock-based locking.
    """

    def __init__(self, registry_path: Optional[str] = None):
        if registry_path is None:
            registry_path = os.path.expanduser("~/.pyturbo/registry.json")

        self._path = Path(registry_path)
        self._lock_path = self._path.parent / "registry.lock"

        # Auto-create directory and empty registry on first run
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if not self._path.exists():
            self._write_data(RegistryData())

    @contextmanager
    def _lock(self) -> Generator[None, None, None]:
        """Acquire flock on the lock file."""
        fd = os.open(str(self._lock_path), os.O_CREAT | os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _read_data(self) -> RegistryData:
        """Read registry JSON file."""
        if not self._path.exists():
            return RegistryData()

        with open(self._path) as f:
            raw = json.load(f)

        jobs = [RegistryEntry(**j) for j in raw.get("jobs", [])]
        return RegistryData(
            version=raw.get("version", 1),
            updated_at=raw.get("updated_at", ""),
            jobs=jobs,
        )

    def _write_data(self, data: RegistryData) -> None:
        """Write registry JSON file."""
        data.updated_at = datetime.now().isoformat()
        raw = {
            "version": data.version,
            "updated_at": data.updated_at,
            "jobs": [
                {
                    "job_id": j.job_id,
                    "gpu_ids": j.gpu_ids,
                    "pid": j.pid,
                    "started_at": j.started_at,
                    "command": j.command,
                }
                for j in data.jobs
            ],
        }
        # Atomic write via temp file
        tmp_path = self._path.with_suffix(".tmp")
        with open(tmp_path, "w") as f:
            json.dump(raw, f, indent=2)
        tmp_path.replace(self._path)

    def _cleanup_stale(self, data: RegistryData) -> int:
        """Remove entries whose PID is no longer alive. Returns count removed."""
        alive = []
        removed = 0

        for entry in data.jobs:
            if self._pid_alive(entry.pid):
                alive.append(entry)
            else:
                logger.info(
                    "[CLEANUP] Removed stale entry job_id=%s, pid=%d (process dead)",
                    entry.job_id,
                    entry.pid,
                )
                removed += 1

        data.jobs = alive
        return removed

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        """Check if a process is alive using os.kill(pid, 0)."""
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            # Process exists but we don't have permission to signal it
            return True

    def register(self, job_id: str, gpu_ids: list[int], command: str = "") -> None:
        """Register a job as occupying the given GPUs."""
        with self._lock():
            data = self._read_data()
            self._cleanup_stale(data)

            entry = RegistryEntry(
                job_id=job_id,
                gpu_ids=gpu_ids,
                pid=os.getpid(),
                started_at=datetime.now().isoformat(),
                command=command,
            )
            data.jobs.append(entry)
            self._write_data(data)

        logger.info(
            "[REGISTRY] Registered job_id=%s, gpu_ids=%s, pid=%d",
            job_id,
            gpu_ids,
            os.getpid(),
        )

    def release(self, job_id: str) -> None:
        """Release a job's GPU allocation."""
        with self._lock():
            data = self._read_data()
            data.jobs = [j for j in data.jobs if j.job_id != job_id]
            self._write_data(data)

        logger.info("[REGISTRY] Released job_id=%s", job_id)

    def get_occupied_gpus(self) -> set[int]:
        """Return set of GPU IDs currently occupied by active jobs."""
        with self._lock():
            data = self._read_data()
            self._cleanup_stale(data)
            self._write_data(data)

            occupied = set()
            for entry in data.jobs:
                occupied.update(entry.gpu_ids)
            return occupied

    def list_jobs(self) -> list[RegistryEntry]:
        """Return list of active registry entries."""
        with self._lock():
            data = self._read_data()
            self._cleanup_stale(data)
            self._write_data(data)
            return list(data.jobs)

    @property
    def path(self) -> str:
        return str(self._path)
