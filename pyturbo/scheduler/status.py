"""Status persistence, message bus, and failure propagation."""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

from .dataclasses import JobStatus

logger = logging.getLogger(__name__)


class StatusStore:
    """Persist task execution status to JSON files.

    File layout: {status_dir}/{task_id}.json
    """

    def __init__(self, status_dir: str = "status"):
        self._dir = Path(status_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    def write(self, status: JobStatus) -> None:
        path = self._dir / f"{status.job_id}.json"
        data = {
            "job_id": status.job_id,
            "state": status.state,
            "start_time": status.start_time,
            "end_time": status.end_time,
            "gpu_ids": status.gpu_ids,
            "exit_code": status.exit_code,
            "retry_count": status.retry_count,
            "artifacts": status.artifacts,
            "wandb_run_id": status.wandb_run_id,
        }
        tmp_path = path.with_suffix(".tmp")
        with open(tmp_path, "w") as f:
            json.dump(data, f, indent=2)
        tmp_path.replace(path)

    def read(self, task_id: str) -> Optional[JobStatus]:
        path = self._dir / f"{task_id}.json"
        if not path.exists():
            return None
        with open(path) as f:
            data = json.load(f)
        return JobStatus(**data)

    def list_all(self) -> list[JobStatus]:
        statuses = []
        for path in self._dir.glob("*.json"):
            with open(path) as f:
                data = json.load(f)
            statuses.append(JobStatus(**data))
        return statuses


class MessageBus(ABC):
    """Abstract message bus for task event publication/subscription."""

    @abstractmethod
    def publish(self, topic: str, payload: dict) -> None: ...

    @abstractmethod
    def subscribe(self, topic: str, callback: Callable[[dict], None]) -> None: ...


class FileMessageBus(MessageBus):
    """File-based message bus using status JSON files."""

    def __init__(self, status_store: StatusStore):
        self._store = status_store
        self._subscribers: dict[str, list[Callable]] = {}

    def publish(self, topic: str, payload: dict) -> None:
        """Write status update and notify subscribers."""
        # Persist the payload as a status file
        if "job_id" not in payload:
            payload["job_id"] = topic.replace("task/", "")

        status = JobStatus(
            job_id=payload.get("job_id", ""),
            state=payload.get("state", "unknown"),
            start_time=payload.get("start_time"),
            end_time=payload.get("end_time"),
            gpu_ids=payload.get("gpu_ids", []),
            exit_code=payload.get("exit_code"),
            retry_count=payload.get("retry_count", 0),
        )
        self._store.write(status)

        # Notify in-process subscribers
        for cb in self._subscribers.get(topic, []):
            cb(payload)

    def subscribe(self, topic: str, callback: Callable[[dict], None]) -> None:
        if topic not in self._subscribers:
            self._subscribers[topic] = []
        self._subscribers[topic].append(callback)


class FailurePropagator:
    """Handle task retry logic and failure propagation.

    Flow: retry -> exceed max_retry -> propagate to downstream -> branch_fail
    """

    @staticmethod
    def should_retry(exit_code: int, retry_count: int, max_retry: int) -> bool:
        return exit_code != 0 and retry_count < max_retry

    @staticmethod
    def handle_failure(
        task_id: str,
        exit_code: int,
        retry_count: int,
        max_retry: int,
        dag: Any,
    ) -> str:
        """Process a task failure. Returns the new status string.

        If retries remain, returns 'pending' (re-queue).
        If max retries exceeded, sets 'failed' and propagates to downstream.
        """
        if FailurePropagator.should_retry(exit_code, retry_count, max_retry):
            logger.info(
                "[RETRY] task=%s exit_code=%d retry=%d/%d, re-queuing",
                task_id,
                exit_code,
                retry_count + 1,
                max_retry,
            )
            return "pending"

        # Max retries exceeded
        logger.info(
            "[FAIL] task=%s exceeded max_retry=%d, propagating failure",
            task_id,
            max_retry,
        )
        dag.set_status(task_id, "failed")
        affected = dag.propagate_failure(task_id)
        if affected:
            logger.info(
                "[FAIL] task=%s propagated failure to %s", task_id, affected
            )
        return "failed"
