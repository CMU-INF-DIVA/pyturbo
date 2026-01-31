"""BlockManager for serial group scheduling and mutex detection."""

from __future__ import annotations

import logging
from collections import defaultdict, deque

from .dataclasses import TaskSpec
from .task import TaskGraph

logger = logging.getLogger(__name__)


class BlockManager:
    """Manage serial groups (block_group) and mutex constraints.

    Rules:
    - Tasks in the same block_group execute one at a time (FIFO by priority).
    - DAG dependencies (blockedBy) take precedence over block_group constraints.
    - Tasks with no block_group are unconstrained by this manager.
    """

    def __init__(self, specs: list[TaskSpec]):
        self._groups: dict[str, deque[str]] = defaultdict(deque)

        # Group tasks by block_group, sorted by priority within each group
        grouped: dict[str, list[TaskSpec]] = defaultdict(list)
        for spec in specs:
            if spec.block_group:
                grouped[spec.block_group].append(spec)

        for group_name, group_specs in grouped.items():
            group_specs.sort(key=lambda s: s.priority)
            self._groups[group_name] = deque(s.id for s in group_specs)

    def is_runnable(self, task_id: str, dag: TaskGraph) -> bool:
        """Check if a task can run: DAG deps satisfied AND no same-group task running."""
        spec = dag.get_spec(task_id)

        # DAG dependencies must be satisfied first
        if not dag.deps_satisfied(task_id):
            return False

        # If task has a block_group, check no other task in the group is running
        if spec.block_group:
            group = self._groups.get(spec.block_group, deque())
            for tid in group:
                if tid != task_id and dag.get_status(tid) == "running":
                    return False

        return True

    def filter_runnable(self, candidates: list[TaskSpec], dag: TaskGraph) -> list[TaskSpec]:
        """Filter a list of candidate tasks to only those that are runnable."""
        return [spec for spec in candidates if self.is_runnable(spec.id, dag)]
