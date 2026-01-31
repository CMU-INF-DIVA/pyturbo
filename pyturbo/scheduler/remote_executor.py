"""V3 RemoteExecutor: dispatch tasks to remote machines via GitHub Actions or SSH."""

from __future__ import annotations

import json
import logging
import subprocess
import time
from typing import Optional

from .config import MachineConfig, RemoteConfig
from .dataclasses import Handle, TaskResult, TaskSpec
from .runner import Executor

logger = logging.getLogger(__name__)


class RemoteHandle(Handle):
    """Handle for a remotely running task."""

    def __init__(
        self,
        task_id: str,
        gpu_ids: list[int],
        machine_id: str,
        run_id: Optional[str] = None,
        method: str = "gh",  # "gh" or "ssh"
    ):
        super().__init__(task_id=task_id, gpu_ids=gpu_ids)
        self.machine_id = machine_id
        self.run_id = run_id
        self.method = method


class RemoteExecutor(Executor):
    """Execute tasks on remote machines via GitHub Actions workflow_dispatch or SSH.

    Methods:
    - gh: Trigger GitHub Actions workflow and poll for completion via gh CLI
    - ssh: Direct SSH execution on remote machine
    """

    def __init__(self, config: RemoteConfig, method: str = "gh"):
        self._config = config
        self._method = method

    def run(self, machine_id: str, spec: TaskSpec, gpu_ids: list[int]) -> RemoteHandle:
        if self._method == "gh":
            return self._run_gh(machine_id, spec, gpu_ids)
        elif self._method == "ssh":
            return self._run_ssh(machine_id, spec, gpu_ids)
        else:
            raise ValueError(f"Unknown remote execution method: {self._method}")

    def poll(self, handle: Handle) -> Optional[TaskResult]:
        if not isinstance(handle, RemoteHandle):
            raise TypeError(f"RemoteExecutor expects RemoteHandle, got {type(handle)}")

        if handle.method == "gh":
            return self._poll_gh(handle)
        elif handle.method == "ssh":
            return self._poll_ssh(handle)
        return None

    # --- GitHub Actions ---

    def _run_gh(self, machine_id: str, spec: TaskSpec, gpu_ids: list[int]) -> RemoteHandle:
        """Trigger a GitHub Actions workflow_dispatch."""
        gpu_str = ",".join(map(str, gpu_ids))
        repo = self._config.github_repo
        workflow = self._config.default_workflow

        cmd = [
            "gh", "workflow", "run", workflow,
            "--repo", repo,
            "-f", f"experiment={spec.id}",
            "-f", f"mode=training",
            "-f", f"gpu_count={spec.gpu_required}",
        ]

        logger.info(
            "[REMOTE-GH] Dispatching task=%s to %s: %s",
            spec.id, machine_id, " ".join(cmd),
        )

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(
                "[REMOTE-GH] Failed to dispatch: %s", result.stderr.strip()
            )
            raise RuntimeError(f"gh workflow dispatch failed: {result.stderr}")

        # Try to get the run ID from the most recent run
        run_id = self._get_latest_run_id(repo, workflow)

        return RemoteHandle(
            task_id=spec.id,
            gpu_ids=gpu_ids,
            machine_id=machine_id,
            run_id=run_id,
            method="gh",
        )

    def _poll_gh(self, handle: RemoteHandle) -> Optional[TaskResult]:
        """Poll GitHub Actions run status."""
        if not handle.run_id:
            return None

        repo = self._config.github_repo
        cmd = [
            "gh", "run", "view", handle.run_id,
            "--repo", repo, "--json", "status,conclusion",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.warning(
                "[REMOTE-GH] Failed to poll run %s: %s",
                handle.run_id, result.stderr.strip(),
            )
            return None

        data = json.loads(result.stdout)
        status = data.get("status", "")
        conclusion = data.get("conclusion", "")

        if status != "completed":
            return None

        exit_code = 0 if conclusion == "success" else 1
        logger.info(
            "[REMOTE-GH] Run %s completed: conclusion=%s",
            handle.run_id, conclusion,
        )
        return TaskResult(
            task_id=handle.task_id,
            exit_code=exit_code,
            gpu_ids=handle.gpu_ids,
        )

    def _get_latest_run_id(self, repo: str, workflow: str) -> Optional[str]:
        """Get the most recent workflow run ID."""
        cmd = [
            "gh", "run", "list",
            "--repo", repo,
            "--workflow", workflow,
            "--limit", "1",
            "--json", "databaseId",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            runs = json.loads(result.stdout)
            if runs:
                return str(runs[0]["databaseId"])
        return None

    # --- SSH ---

    def _run_ssh(self, machine_id: str, spec: TaskSpec, gpu_ids: list[int]) -> RemoteHandle:
        """Execute task on remote machine via SSH."""
        machine = self._config.get_machine(machine_id)
        gpu_str = ",".join(map(str, gpu_ids))
        command = spec.command.replace("{gpu_ids}", gpu_str)

        ssh_cmd = self._build_ssh_cmd(machine, command, gpu_str)

        logger.info(
            "[REMOTE-SSH] Dispatching task=%s to %s@%s: %s",
            spec.id, machine.ssh_user, machine.hostname, command,
        )

        proc = subprocess.Popen(
            ssh_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        handle = RemoteHandle(
            task_id=spec.id,
            gpu_ids=gpu_ids,
            machine_id=machine_id,
            run_id=str(proc.pid),
            method="ssh",
        )
        handle._proc = proc  # stash proc for polling
        return handle

    def _poll_ssh(self, handle: RemoteHandle) -> Optional[TaskResult]:
        """Poll SSH process."""
        proc = getattr(handle, "_proc", None)
        if proc is None:
            return None

        retcode = proc.poll()
        if retcode is None:
            return None

        return TaskResult(
            task_id=handle.task_id,
            exit_code=retcode,
            gpu_ids=handle.gpu_ids,
        )

    @staticmethod
    def _build_ssh_cmd(machine: MachineConfig, command: str, gpu_str: str) -> list[str]:
        ssh_cmd = ["ssh"]
        if machine.ssh_key:
            ssh_cmd.extend(["-i", machine.ssh_key])
        ssh_cmd.extend([
            "-o", "StrictHostKeyChecking=no",
            f"{machine.ssh_user}@{machine.hostname}",
            f"CUDA_VISIBLE_DEVICES={gpu_str} {command}",
        ])
        return ssh_cmd
