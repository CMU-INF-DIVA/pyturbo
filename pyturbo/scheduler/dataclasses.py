"""Shared data classes for pyturbo scheduler."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNABLE = "runnable"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    BRANCH_FAIL = "branch_fail"
    BLOCKED = "blocked"


@dataclass
class TaskSpec:
    """Minimal atomic task definition."""

    id: str
    subject: str
    command: str
    gpu_required: int = 4
    description: str = ""
    activeForm: str = ""
    status: str = "pending"
    deps: list[str] = field(default_factory=list)       # blockedBy
    blocks: list[str] = field(default_factory=list)
    block_group: Optional[str] = None
    max_retry: int = 1
    tags: list[str] = field(default_factory=list)
    priority: int = 1
    status_output: Optional[str] = None


@dataclass
class GPUState:
    """Real-time GPU status from pynvml."""

    id: int
    mem_used_mb: float
    mem_total_mb: float
    utilization_pct: int
    process_count: int
    available: bool = False  # computed field


@dataclass
class RegistryEntry:
    """A single job entry in the GPU registry."""

    job_id: str
    gpu_ids: list[int]
    pid: int
    started_at: str
    command: str = ""


@dataclass
class RegistryData:
    """Top-level registry JSON structure."""

    version: int = 1
    updated_at: str = ""
    jobs: list[RegistryEntry] = field(default_factory=list)


@dataclass
class JobStatus:
    """Persisted task execution status."""

    job_id: str
    state: str = "pending"
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    gpu_ids: list[int] = field(default_factory=list)
    exit_code: Optional[int] = None
    retry_count: int = 0
    artifacts: dict[str, Any] = field(default_factory=dict)
    wandb_run_id: Optional[str] = None


@dataclass
class ProcessHandle:
    """Handle for a locally running subprocess."""

    proc: subprocess.Popen
    task_id: str
    gpu_ids: list[int] = field(default_factory=list)


@dataclass
class Handle:
    """Abstract handle base - used by RemoteExecutor in v3."""

    task_id: str
    gpu_ids: list[int] = field(default_factory=list)


@dataclass
class TaskResult:
    """Result of a completed task execution."""

    task_id: str
    exit_code: int
    gpu_ids: list[int] = field(default_factory=list)
    end_time: str = field(default_factory=lambda: datetime.now().isoformat())
