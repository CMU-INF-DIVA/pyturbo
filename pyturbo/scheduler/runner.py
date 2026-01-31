"""Executor abstractions, LocalExecutor, and DAGRunner core loop."""

from __future__ import annotations

import logging
import os
import select
import subprocess
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from .allocator import ResourceAllocator
from .block import BlockManager
from .dataclasses import Handle, JobStatus, ProcessHandle, TaskResult, TaskSpec
from .status import FailurePropagator, MessageBus, StatusStore
from .task import TaskGraph

logger = logging.getLogger(__name__)


# === Abstract interfaces (v2/v3 shared) ===


class Executor(ABC):
    """Abstract executor for running tasks."""

    @abstractmethod
    def run(self, machine_id: str, spec: TaskSpec, gpu_ids: list[int]) -> Handle:
        ...

    @abstractmethod
    def poll(self, handle: Handle) -> Optional[TaskResult]:
        """Non-blocking check if the task has completed. Returns None if still running."""
        ...


# === v2 implementation: local subprocess ===


class LocalExecutor(Executor):
    """Execute tasks as local subprocesses with CUDA_VISIBLE_DEVICES isolation."""

    def run(self, machine_id: str, spec: TaskSpec, gpu_ids: list[int]) -> ProcessHandle:
        gpu_str = ",".join(map(str, gpu_ids))
        command = spec.command.replace("{gpu_ids}", gpu_str)

        env = os.environ.copy()
        env["CUDA_VISIBLE_DEVICES"] = gpu_str

        logger.info(
            "[EXEC] Starting task=%s on GPU %s: %s", spec.id, gpu_str, command
        )

        proc = subprocess.Popen(
            command,
            shell=True,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        return ProcessHandle(proc=proc, task_id=spec.id, gpu_ids=gpu_ids)

    def poll(self, handle: Handle) -> Optional[TaskResult]:
        if not isinstance(handle, ProcessHandle):
            raise TypeError(f"LocalExecutor expects ProcessHandle, got {type(handle)}")

        retcode = handle.proc.poll()
        if retcode is None:
            return None  # still running

        return TaskResult(
            task_id=handle.task_id,
            exit_code=retcode,
            gpu_ids=handle.gpu_ids,
        )


# === DAGRunner: core scheduling loop ===


class DAGRunner:
    """Orchestrate DAG task execution. v2/v3 agnostic via Executor/MessageBus injection."""

    def __init__(
        self,
        executor: Executor,
        bus: MessageBus,
        allocator: ResourceAllocator,
        status_store: StatusStore,
        poll_interval: int = 30,
        max_wait: int = 3600,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        self.executor = executor
        self.bus = bus
        self.allocator = allocator
        self.status_store = status_store
        self.poll_interval = poll_interval
        self.max_wait = max_wait
        self.dry_run = dry_run
        self.verbose = verbose

    def run(self, dag: TaskGraph) -> int:
        """Run the full DAG to completion. Returns exit code (0=all success, 1=some failed)."""
        block_mgr = BlockManager(dag.get_all_specs())
        active: dict[str, Handle] = {}  # task_id -> handle
        retry_counts: dict[str, int] = {}
        wait_start: Optional[float] = None

        logger.info("[DAG] Loaded %d tasks: %s", len(dag.get_all_specs()),
                     [s.id for s in dag.get_all_specs()])
        logger.info("[DAG] Summary: %s", dag.summary())

        if self.dry_run:
            return self._dry_run(dag, block_mgr)

        while not dag.all_done():
            # 1. Check completed tasks
            completed = []
            for task_id, handle in active.items():
                result = self.executor.poll(handle)
                if result is not None:
                    completed.append((task_id, result))

            for task_id, result in completed:
                active.pop(task_id)
                self._handle_completion(task_id, result, dag, retry_counts)

            # 2. Find and dispatch runnable tasks
            runnable = dag.get_runnable()
            runnable = block_mgr.filter_runnable(runnable, dag)

            dispatched_any = False
            for spec in runnable:
                if spec.id in active:
                    continue

                gpu_ids = self.allocator.try_acquire(spec.gpu_required)
                if gpu_ids is None:
                    continue

                # Register and dispatch
                self.allocator.acquire(
                    job_id=spec.id, gpu_ids=gpu_ids, command=spec.command
                )
                handle = self.executor.run("localhost", spec, gpu_ids)
                active[spec.id] = handle
                dag.set_status(spec.id, "running")

                status = JobStatus(
                    job_id=spec.id,
                    state="running",
                    start_time=datetime.now().isoformat(),
                    gpu_ids=gpu_ids,
                    retry_count=retry_counts.get(spec.id, 0),
                )
                self.status_store.write(status)
                self.bus.publish(f"task/{spec.id}", {"state": "running", "gpu_ids": gpu_ids})

                logger.info(
                    "[DISPATCH] task=%s -> GPU %s (pid=%s)",
                    spec.id,
                    gpu_ids,
                    getattr(handle, 'proc', {}).pid if isinstance(handle, ProcessHandle) else "?",
                )
                dispatched_any = True
                wait_start = None  # reset wait timer on successful dispatch

            # 3. If nothing dispatched and nothing active, check wait timeout
            if not dispatched_any and not active:
                if runnable:
                    # Tasks exist but no GPU available
                    if wait_start is None:
                        wait_start = time.time()

                    elapsed = time.time() - wait_start
                    if elapsed > self.max_wait:
                        logger.error(
                            "[TIMEOUT] Waited %.0fs (max=%ds), exiting",
                            elapsed,
                            self.max_wait,
                        )
                        return 75

                    avail = self.allocator.get_available_gpus()
                    logger.info(
                        "[WAIT] elapsed=%.0fs, available_gpus=%d, runnable=%d",
                        elapsed,
                        len(avail),
                        len(runnable),
                    )

            # 4. Sleep before next iteration
            time.sleep(min(self.poll_interval, 5) if active else self.poll_interval)

        # Final summary
        summary = dag.summary()
        logger.info("[DAG] Completed. Summary: %s", summary)

        if summary.get("failed", 0) > 0 or summary.get("branch_fail", 0) > 0:
            return 1
        return 0

    def _handle_completion(
        self,
        task_id: str,
        result: TaskResult,
        dag: TaskGraph,
        retry_counts: dict[str, int],
    ) -> None:
        """Handle a completed task: update status, retry or propagate failure."""
        spec = dag.get_spec(task_id)

        # Release GPU
        self.allocator.release(task_id)

        if result.exit_code == 0:
            dag.set_status(task_id, "success")
            status = JobStatus(
                job_id=task_id,
                state="success",
                end_time=result.end_time,
                gpu_ids=result.gpu_ids,
                exit_code=0,
                retry_count=retry_counts.get(task_id, 0),
            )
            self.status_store.write(status)
            self.bus.publish(f"task/{task_id}", {"state": "success"})
            logger.info("[DONE] task=%s completed successfully", task_id)
        else:
            count = retry_counts.get(task_id, 0)
            new_status = FailurePropagator.handle_failure(
                task_id, result.exit_code, count, spec.max_retry, dag
            )
            if new_status == "pending":
                retry_counts[task_id] = count + 1
                dag.set_status(task_id, "pending")
            else:
                status = JobStatus(
                    job_id=task_id,
                    state="failed",
                    end_time=result.end_time,
                    gpu_ids=result.gpu_ids,
                    exit_code=result.exit_code,
                    retry_count=count,
                )
                self.status_store.write(status)

    def _dry_run(self, dag: TaskGraph, block_mgr: BlockManager) -> int:
        """Print allocation plan without executing."""
        logger.info("[DRY-RUN] Task execution plan:")

        runnable = dag.get_runnable()
        runnable = block_mgr.filter_runnable(runnable, dag)

        available = self.allocator.get_available_gpus()
        logger.info("[DRY-RUN] Available GPUs: %s", available)

        for spec in runnable:
            if len(available) >= spec.gpu_required:
                assigned = available[: spec.gpu_required]
                available = available[spec.gpu_required:]
                logger.info(
                    "[DRY-RUN] task=%s -> GPU %s (gpu_required=%d)",
                    spec.id,
                    assigned,
                    spec.gpu_required,
                )
            else:
                logger.info(
                    "[DRY-RUN] task=%s -> WAITING (need %d, available %d)",
                    spec.id,
                    spec.gpu_required,
                    len(available),
                )

        pending = [s for s in dag.get_all_specs() if s.id not in {r.id for r in runnable}]
        for spec in pending:
            logger.info("[DRY-RUN] task=%s -> BLOCKED (deps not satisfied)", spec.id)

        return 0
