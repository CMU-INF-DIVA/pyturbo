"""Integration tests for DAGRunner with mock executor."""

import json
from unittest.mock import MagicMock

import pytest

from pyturbo.scheduler.dataclasses import Handle, TaskResult
from pyturbo.scheduler.runner import DAGRunner, Executor
from pyturbo.scheduler.status import FileMessageBus, StatusStore
from pyturbo.scheduler.task import TaskGraph, TaskLoader


class MockExecutor(Executor):
    """Mock executor that immediately returns success."""

    def __init__(self, results=None):
        self._results = results or {}  # task_id -> exit_code
        self._running = {}

    def run(self, machine_id, spec, gpu_ids):
        handle = Handle(task_id=spec.id, gpu_ids=gpu_ids)
        self._running[spec.id] = handle
        return handle

    def poll(self, handle):
        exit_code = self._results.get(handle.task_id, 0)
        return TaskResult(
            task_id=handle.task_id,
            exit_code=exit_code,
            gpu_ids=handle.gpu_ids,
        )


@pytest.fixture
def dag_file(tmp_path):
    data = [
        {
            "id": "T1-train",
            "subject": "Train T1",
            "command": "echo train",
            "gpu_required": 4,
            "blockedBy": [],
            "blocks": ["T1-eval"],
            "priority": 1,
        },
        {
            "id": "T1-eval",
            "subject": "Eval T1",
            "command": "echo eval",
            "gpu_required": 4,
            "blockedBy": ["T1-train"],
            "blocks": [],
            "priority": 2,
        },
    ]
    path = tmp_path / "dag.json"
    path.write_text(json.dumps(data))
    return str(path)


@pytest.fixture
def mock_allocator():
    alloc = MagicMock()
    alloc.get_available_gpus.return_value = [0, 1, 2, 3]
    alloc.try_acquire.return_value = [0, 1, 2, 3]
    return alloc


class TestDAGRunner:
    def test_simple_dag_all_success(self, dag_file, mock_allocator, tmp_path):
        specs = TaskLoader.load(dag_file)
        dag = TaskGraph(specs)
        executor = MockExecutor()
        status_store = StatusStore(str(tmp_path / "status"))
        bus = FileMessageBus(status_store)

        runner = DAGRunner(
            executor=executor,
            bus=bus,
            allocator=mock_allocator,
            status_store=status_store,
            poll_interval=1,
        )

        exit_code = runner.run(dag)
        assert exit_code == 0
        assert dag.get_status("T1-train") == "success"
        assert dag.get_status("T1-eval") == "success"

    def test_dag_with_failure(self, dag_file, mock_allocator, tmp_path):
        specs = TaskLoader.load(dag_file)
        dag = TaskGraph(specs)
        executor = MockExecutor(results={"T1-train": 1})  # T1-train fails
        status_store = StatusStore(str(tmp_path / "status"))
        bus = FileMessageBus(status_store)

        runner = DAGRunner(
            executor=executor,
            bus=bus,
            allocator=mock_allocator,
            status_store=status_store,
            poll_interval=1,
        )

        exit_code = runner.run(dag)
        assert exit_code == 1
        assert dag.get_status("T1-train") == "failed"
        assert dag.get_status("T1-eval") == "branch_fail"

    def test_dry_run(self, dag_file, mock_allocator, tmp_path):
        specs = TaskLoader.load(dag_file)
        dag = TaskGraph(specs)
        executor = MockExecutor()
        status_store = StatusStore(str(tmp_path / "status"))
        bus = FileMessageBus(status_store)

        runner = DAGRunner(
            executor=executor,
            bus=bus,
            allocator=mock_allocator,
            status_store=status_store,
            dry_run=True,
        )

        exit_code = runner.run(dag)
        assert exit_code == 0
        # Tasks should NOT have been executed
        assert dag.get_status("T1-train") == "pending"

    def test_dag_with_retry(self, tmp_path, mock_allocator):
        data = [
            {
                "id": "T1-train",
                "subject": "Train T1",
                "command": "echo train",
                "gpu_required": 4,
                "max_retry": 2,
                "blockedBy": [],
                "blocks": [],
            },
        ]
        path = tmp_path / "retry.json"
        path.write_text(json.dumps(data))

        specs = TaskLoader.load(str(path))
        dag = TaskGraph(specs)

        # Executor fails twice then succeeds
        call_count = {"count": 0}
        original_results = {}

        class RetryExecutor(Executor):
            def run(self, machine_id, spec, gpu_ids):
                return Handle(task_id=spec.id, gpu_ids=gpu_ids)

            def poll(self, handle):
                call_count["count"] += 1
                exit_code = 1 if call_count["count"] <= 2 else 0
                return TaskResult(
                    task_id=handle.task_id,
                    exit_code=exit_code,
                    gpu_ids=handle.gpu_ids,
                )

        executor = RetryExecutor()
        status_store = StatusStore(str(tmp_path / "status"))
        bus = FileMessageBus(status_store)

        runner = DAGRunner(
            executor=executor,
            bus=bus,
            allocator=mock_allocator,
            status_store=status_store,
            poll_interval=1,
        )

        exit_code = runner.run(dag)
        assert exit_code == 0
        assert dag.get_status("T1-train") == "success"
