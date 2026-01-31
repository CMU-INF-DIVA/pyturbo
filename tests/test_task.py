"""Tests for TaskLoader and TaskGraph."""

import json
import os
import tempfile

import pytest

from pyturbo.scheduler.dataclasses import TaskSpec
from pyturbo.scheduler.task import TaskGraph, TaskLoader


@pytest.fixture
def single_task_file(tmp_path):
    data = {
        "id": "T1-train",
        "subject": "Train T1",
        "command": "echo hello",
        "gpu_required": 4,
        "blockedBy": [],
        "blocks": ["T1-eval"],
    }
    path = tmp_path / "single.json"
    path.write_text(json.dumps(data))
    return str(path)


@pytest.fixture
def dag_tasks_file(tmp_path):
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
        {
            "id": "T3-train",
            "subject": "Train T3",
            "command": "echo train3",
            "gpu_required": 4,
            "blockedBy": [],
            "blocks": ["T3-eval"],
            "block_group": "group-2",
            "priority": 1,
        },
        {
            "id": "T3-eval",
            "subject": "Eval T3",
            "command": "echo eval3",
            "gpu_required": 4,
            "blockedBy": ["T3-train"],
            "blocks": [],
            "block_group": "group-2",
            "priority": 2,
        },
    ]
    path = tmp_path / "dag.json"
    path.write_text(json.dumps(data))
    return str(path)


@pytest.fixture
def cycle_tasks_file(tmp_path):
    data = [
        {"id": "A", "subject": "A", "command": "echo a", "blockedBy": ["B"]},
        {"id": "B", "subject": "B", "command": "echo b", "blockedBy": ["A"]},
    ]
    path = tmp_path / "cycle.json"
    path.write_text(json.dumps(data))
    return str(path)


class TestTaskLoader:
    def test_load_single_task(self, single_task_file):
        specs = TaskLoader.load(single_task_file)
        assert len(specs) == 1
        assert specs[0].id == "T1-train"
        assert specs[0].gpu_required == 4
        assert specs[0].deps == []
        assert specs[0].blocks == ["T1-eval"]

    def test_load_dag_tasks(self, dag_tasks_file):
        specs = TaskLoader.load(dag_tasks_file)
        assert len(specs) == 4

    def test_load_nonexistent_file(self):
        with pytest.raises(FileNotFoundError):
            TaskLoader.load("/nonexistent/path.json")

    def test_blockedby_mapped_to_deps(self, single_task_file):
        specs = TaskLoader.load(single_task_file)
        assert specs[0].deps == []


class TestTaskGraph:
    def test_build_graph(self, dag_tasks_file):
        specs = TaskLoader.load(dag_tasks_file)
        dag = TaskGraph(specs)
        assert len(dag.get_all_specs()) == 4

    def test_cycle_detection(self, cycle_tasks_file):
        specs = TaskLoader.load(cycle_tasks_file)
        with pytest.raises(ValueError, match="Cycle detected"):
            TaskGraph(specs)

    def test_get_runnable_initial(self, dag_tasks_file):
        specs = TaskLoader.load(dag_tasks_file)
        dag = TaskGraph(specs)
        runnable = dag.get_runnable()
        runnable_ids = [s.id for s in runnable]
        assert "T1-train" in runnable_ids
        assert "T3-train" in runnable_ids
        assert "T1-eval" not in runnable_ids
        assert "T3-eval" not in runnable_ids

    def test_deps_satisfied(self, dag_tasks_file):
        specs = TaskLoader.load(dag_tasks_file)
        dag = TaskGraph(specs)
        assert dag.deps_satisfied("T1-train") is True
        assert dag.deps_satisfied("T1-eval") is False

    def test_deps_satisfied_after_completion(self, dag_tasks_file):
        specs = TaskLoader.load(dag_tasks_file)
        dag = TaskGraph(specs)
        dag.set_status("T1-train", "success")
        assert dag.deps_satisfied("T1-eval") is True

    def test_all_done(self, dag_tasks_file):
        specs = TaskLoader.load(dag_tasks_file)
        dag = TaskGraph(specs)
        assert dag.all_done() is False

        for spec in specs:
            dag.set_status(spec.id, "success")
        assert dag.all_done() is True

    def test_propagate_failure(self, dag_tasks_file):
        specs = TaskLoader.load(dag_tasks_file)
        dag = TaskGraph(specs)
        dag.set_status("T1-train", "failed")
        affected = dag.propagate_failure("T1-train")
        assert "T1-eval" in affected
        assert dag.get_status("T1-eval") == "branch_fail"

    def test_runnable_sorted_by_priority(self, dag_tasks_file):
        specs = TaskLoader.load(dag_tasks_file)
        dag = TaskGraph(specs)
        runnable = dag.get_runnable()
        # Both T1-train and T3-train have priority 1
        assert all(s.priority == 1 for s in runnable)

    def test_summary(self, dag_tasks_file):
        specs = TaskLoader.load(dag_tasks_file)
        dag = TaskGraph(specs)
        summary = dag.summary()
        assert summary["pending"] == 4

    def test_unknown_dependency(self, tmp_path):
        data = [
            {"id": "A", "subject": "A", "command": "echo a", "blockedBy": ["NONEXISTENT"]},
        ]
        path = tmp_path / "bad.json"
        path.write_text(json.dumps(data))
        specs = TaskLoader.load(str(path))
        with pytest.raises(ValueError, match="unknown task"):
            TaskGraph(specs)
