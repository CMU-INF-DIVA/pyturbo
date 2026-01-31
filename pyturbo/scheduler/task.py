"""Task loading, DAG topology, and dependency management."""

from __future__ import annotations

import json
import logging
from collections import defaultdict, deque
from pathlib import Path
from typing import Union

from .dataclasses import TaskSpec, TaskStatus

logger = logging.getLogger(__name__)


class TaskLoader:
    """Load TaskSpec instances from JSON files."""

    @staticmethod
    def load(path: Union[str, Path]) -> list[TaskSpec]:
        """Load task definitions from a JSON file.

        Supports both single task (dict) and multiple tasks (list of dicts).
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Task definition file not found: {path}")

        with open(path) as f:
            data = json.load(f)

        if isinstance(data, dict):
            data = [data]

        specs = []
        for item in data:
            # Map blockedBy -> deps for compatibility
            if "blockedBy" in item and "deps" not in item:
                item["deps"] = item.pop("blockedBy")
            elif "blockedBy" in item:
                item.pop("blockedBy")

            # Remove unknown fields
            known_fields = {f.name for f in TaskSpec.__dataclass_fields__.values()}
            filtered = {k: v for k, v in item.items() if k in known_fields}

            specs.append(TaskSpec(**filtered))

        return specs


class TaskGraph:
    """DAG-based task dependency graph with status tracking."""

    def __init__(self, specs: list[TaskSpec]):
        self._specs: dict[str, TaskSpec] = {}
        self._status: dict[str, TaskStatus] = {}
        self._deps: dict[str, set[str]] = defaultdict(set)      # task -> its dependencies
        self._dependents: dict[str, set[str]] = defaultdict(set) # task -> tasks that depend on it

        for spec in specs:
            self._specs[spec.id] = spec
            self._status[spec.id] = TaskStatus(spec.status) if spec.status != "pending" else TaskStatus.PENDING

        # Build edges
        for spec in specs:
            for dep_id in spec.deps:
                if dep_id not in self._specs:
                    raise ValueError(
                        f"Task '{spec.id}' depends on unknown task '{dep_id}'"
                    )
                self._deps[spec.id].add(dep_id)
                self._dependents[dep_id].add(spec.id)

            for block_id in spec.blocks:
                if block_id in self._specs:
                    self._deps[block_id].add(spec.id)
                    self._dependents[spec.id].add(block_id)

        self._detect_cycle()

    def _detect_cycle(self) -> None:
        """Detect cycles using Kahn's algorithm."""
        in_degree = {tid: len(self._deps[tid]) for tid in self._specs}
        queue = deque(tid for tid, deg in in_degree.items() if deg == 0)
        visited = 0

        while queue:
            node = queue.popleft()
            visited += 1
            for dependent in self._dependents[node]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if visited != len(self._specs):
            raise ValueError("Cycle detected in task dependency graph")

    def get_spec(self, task_id: str) -> TaskSpec:
        return self._specs[task_id]

    def get_status(self, task_id: str) -> str:
        return self._status[task_id].value

    def set_status(self, task_id: str, status: str) -> None:
        self._status[task_id] = TaskStatus(status)

    def deps_satisfied(self, task_id: str) -> bool:
        """Check if all dependencies of a task have succeeded."""
        for dep_id in self._deps[task_id]:
            if self._status[dep_id] != TaskStatus.SUCCESS:
                return False
        return True

    def get_runnable(self) -> list[TaskSpec]:
        """Return tasks that are pending and have all deps satisfied.

        Sorted by priority (lower number = higher priority).
        """
        runnable = []
        for tid, status in self._status.items():
            if status == TaskStatus.PENDING and self.deps_satisfied(tid):
                runnable.append(self._specs[tid])

        runnable.sort(key=lambda s: s.priority)
        return runnable

    def get_all_specs(self) -> list[TaskSpec]:
        return list(self._specs.values())

    def all_done(self) -> bool:
        """Check if all tasks are in a terminal state (success/failed/branch_fail)."""
        terminal = {TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.BRANCH_FAIL}
        return all(s in terminal for s in self._status.values())

    def propagate_failure(self, task_id: str) -> list[str]:
        """Propagate failure to all downstream tasks. Returns list of affected task IDs."""
        affected = []
        queue = deque(self._dependents[task_id])

        while queue:
            tid = queue.popleft()
            if self._status[tid] in (TaskStatus.PENDING, TaskStatus.RUNNABLE, TaskStatus.BLOCKED):
                self._status[tid] = TaskStatus.BRANCH_FAIL
                affected.append(tid)
                queue.extend(self._dependents[tid])

        if affected:
            logger.info(
                "[FAIL] task=%s propagating failure to %s", task_id, affected
            )

        return affected

    def summary(self) -> dict[str, int]:
        """Return count of tasks by status."""
        counts: dict[str, int] = defaultdict(int)
        for status in self._status.values():
            counts[status.value] += 1
        return dict(counts)
