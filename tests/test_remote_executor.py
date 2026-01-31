"""Tests for RemoteExecutor (using mocks)."""

import json
from unittest.mock import MagicMock, patch

import pytest

from pyturbo.scheduler.config import MachineConfig, RemoteConfig, WandBConfig
from pyturbo.scheduler.dataclasses import TaskSpec
from pyturbo.scheduler.remote_executor import RemoteExecutor, RemoteHandle


@pytest.fixture
def remote_config():
    return RemoteConfig(
        machines=[
            MachineConfig(
                machine_id="gpu-server-1",
                hostname="10.0.0.1",
                gpu_count=8,
                runner_label="gpu",
                ssh_user="user",
            ),
        ],
        wandb=WandBConfig(project="test-project"),
        github_repo="owner/repo",
        default_workflow="run-experiment.yml",
    )


@pytest.fixture
def sample_spec():
    return TaskSpec(
        id="T1-train",
        subject="Train T1",
        command="echo train {gpu_ids}",
        gpu_required=4,
    )


class TestRemoteExecutorGH:
    @patch("subprocess.run")
    def test_run_gh_dispatches_workflow(self, mock_run, remote_config, sample_spec):
        # Mock successful dispatch
        mock_run.return_value = MagicMock(returncode=0, stdout='[{"databaseId": 12345}]', stderr="")

        executor = RemoteExecutor(remote_config, method="gh")
        handle = executor.run("gpu-server-1", sample_spec, [0, 1, 2, 3])

        assert isinstance(handle, RemoteHandle)
        assert handle.task_id == "T1-train"
        assert handle.machine_id == "gpu-server-1"
        assert handle.method == "gh"
        assert mock_run.called

    @patch("subprocess.run")
    def test_run_gh_failure(self, mock_run, remote_config, sample_spec):
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="auth failed")

        executor = RemoteExecutor(remote_config, method="gh")
        with pytest.raises(RuntimeError, match="gh workflow dispatch failed"):
            executor.run("gpu-server-1", sample_spec, [0, 1, 2, 3])

    @patch("subprocess.run")
    def test_poll_gh_completed_success(self, mock_run, remote_config):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"status": "completed", "conclusion": "success"}),
        )

        executor = RemoteExecutor(remote_config, method="gh")
        handle = RemoteHandle(
            task_id="T1-train",
            gpu_ids=[0, 1, 2, 3],
            machine_id="gpu-server-1",
            run_id="12345",
            method="gh",
        )

        result = executor.poll(handle)
        assert result is not None
        assert result.exit_code == 0
        assert result.task_id == "T1-train"

    @patch("subprocess.run")
    def test_poll_gh_in_progress(self, mock_run, remote_config):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"status": "in_progress", "conclusion": ""}),
        )

        executor = RemoteExecutor(remote_config, method="gh")
        handle = RemoteHandle(
            task_id="T1-train",
            gpu_ids=[0, 1, 2, 3],
            machine_id="gpu-server-1",
            run_id="12345",
            method="gh",
        )

        result = executor.poll(handle)
        assert result is None  # still running

    @patch("subprocess.run")
    def test_poll_gh_failure(self, mock_run, remote_config):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"status": "completed", "conclusion": "failure"}),
        )

        executor = RemoteExecutor(remote_config, method="gh")
        handle = RemoteHandle(
            task_id="T1-train",
            gpu_ids=[0, 1, 2, 3],
            machine_id="gpu-server-1",
            run_id="12345",
            method="gh",
        )

        result = executor.poll(handle)
        assert result is not None
        assert result.exit_code == 1


class TestRemoteExecutorSSH:
    @patch("subprocess.Popen")
    def test_run_ssh(self, mock_popen, remote_config, sample_spec):
        mock_proc = MagicMock()
        mock_proc.pid = 99999
        mock_popen.return_value = mock_proc

        executor = RemoteExecutor(remote_config, method="ssh")
        handle = executor.run("gpu-server-1", sample_spec, [0, 1, 2, 3])

        assert isinstance(handle, RemoteHandle)
        assert handle.task_id == "T1-train"
        assert handle.method == "ssh"
        mock_popen.assert_called_once()

        # Verify SSH command contains the right host
        call_args = mock_popen.call_args
        ssh_cmd = call_args[0][0]
        assert "user@10.0.0.1" in ssh_cmd

    @patch("subprocess.Popen")
    def test_poll_ssh_completed(self, mock_popen, remote_config, sample_spec):
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 0
        mock_popen.return_value = mock_proc

        executor = RemoteExecutor(remote_config, method="ssh")
        handle = executor.run("gpu-server-1", sample_spec, [0, 1, 2, 3])

        result = executor.poll(handle)
        assert result is not None
        assert result.exit_code == 0


class TestRemoteConfig:
    def test_load_config(self, tmp_path):
        config_data = {
            "machines": [
                {
                    "machine_id": "server-1",
                    "hostname": "10.0.0.1",
                    "gpu_count": 8,
                }
            ],
            "wandb": {"project": "test-project"},
            "github_repo": "owner/repo",
        }
        path = tmp_path / "config.json"
        path.write_text(json.dumps(config_data))

        config = RemoteConfig.load(str(path))
        assert len(config.machines) == 1
        assert config.machines[0].machine_id == "server-1"
        assert config.wandb.project == "test-project"

    def test_get_machine(self, remote_config):
        machine = remote_config.get_machine("gpu-server-1")
        assert machine.hostname == "10.0.0.1"

    def test_get_machine_not_found(self, remote_config):
        with pytest.raises(ValueError, match="not found"):
            remote_config.get_machine("nonexistent")

    def test_load_nonexistent_config(self):
        with pytest.raises(FileNotFoundError):
            RemoteConfig.load("/nonexistent/config.json")
