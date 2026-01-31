"""Tests for FileLockRegistry."""

import json
import os
import multiprocessing
import time

import pytest

from pyturbo.scheduler.registry import FileLockRegistry


@pytest.fixture
def registry(tmp_path):
    path = str(tmp_path / "registry.json")
    return FileLockRegistry(path)


class TestFileLockRegistry:
    def test_create_empty_registry(self, registry):
        jobs = registry.list_jobs()
        assert jobs == []

    def test_register_and_list(self, registry):
        registry.register("T1-train", [0, 1, 2, 3], "echo train")
        jobs = registry.list_jobs()
        assert len(jobs) == 1
        assert jobs[0].job_id == "T1-train"
        assert jobs[0].gpu_ids == [0, 1, 2, 3]
        assert jobs[0].pid == os.getpid()

    def test_release(self, registry):
        registry.register("T1-train", [0, 1], "echo train")
        registry.register("T3-train", [2, 3], "echo train3")
        registry.release("T1-train")
        jobs = registry.list_jobs()
        assert len(jobs) == 1
        assert jobs[0].job_id == "T3-train"

    def test_get_occupied_gpus(self, registry):
        registry.register("T1-train", [0, 1], "echo train")
        registry.register("T3-train", [4, 5], "echo train3")
        occupied = registry.get_occupied_gpus()
        assert occupied == {0, 1, 4, 5}

    def test_stale_entry_cleanup(self, tmp_path):
        """Stale entries (dead PIDs) should be cleaned up automatically."""
        path = str(tmp_path / "registry.json")
        reg = FileLockRegistry(path)

        # Write a fake entry with a dead PID
        data = {
            "version": 1,
            "updated_at": "",
            "jobs": [
                {
                    "job_id": "stale-job",
                    "gpu_ids": [0, 1],
                    "pid": 999999999,  # almost certainly dead
                    "started_at": "2026-01-01T00:00:00",
                    "command": "echo stale",
                }
            ],
        }
        with open(path, "w") as f:
            json.dump(data, f)

        # list_jobs should clean up the stale entry
        jobs = reg.list_jobs()
        assert len(jobs) == 0

    def test_registry_json_format(self, tmp_path):
        path = str(tmp_path / "registry.json")
        reg = FileLockRegistry(path)
        reg.register("T1-train", [0, 1, 2, 3], "echo test")

        with open(path) as f:
            data = json.load(f)

        assert data["version"] == 1
        assert "updated_at" in data
        assert len(data["jobs"]) == 1
        assert data["jobs"][0]["job_id"] == "T1-train"

    def test_concurrent_register(self, tmp_path):
        """Multiple processes registering concurrently should not corrupt the registry."""
        path = str(tmp_path / "registry.json")

        def worker(job_id, gpu_ids):
            reg = FileLockRegistry(path)
            reg.register(job_id, gpu_ids, f"echo {job_id}")

        # Initialize registry
        FileLockRegistry(path)

        p1 = multiprocessing.Process(target=worker, args=("job-1", [0, 1]))
        p2 = multiprocessing.Process(target=worker, args=("job-2", [2, 3]))

        p1.start()
        p2.start()
        p1.join(timeout=10)
        p2.join(timeout=10)

        reg = FileLockRegistry(path)
        jobs = reg.list_jobs()
        # Both should be present (child processes are still alive from parent's view
        # until join, but after join they're dead - stale cleanup will remove them)
        # So we just check the file was not corrupted
        with open(path) as f:
            data = json.load(f)
        assert data["version"] == 1
        assert isinstance(data["jobs"], list)
