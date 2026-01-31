"""Multi-machine configuration for V3 remote execution."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class MachineConfig:
    """Configuration for a single remote machine."""

    machine_id: str
    hostname: str
    gpu_count: int
    runner_label: str = ""  # GitHub Actions self-hosted runner label
    ssh_user: Optional[str] = None
    ssh_key: Optional[str] = None
    work_dir: str = "~"


@dataclass
class WandBConfig:
    """WandB integration configuration."""

    project: str = "dsta-stance"
    entity: Optional[str] = None
    poll_interval: int = 30  # seconds between status checks


@dataclass
class RemoteConfig:
    """Top-level V3 remote configuration."""

    machines: list[MachineConfig] = field(default_factory=list)
    wandb: WandBConfig = field(default_factory=WandBConfig)
    github_repo: str = ""  # e.g. "owner/repo"
    default_workflow: str = "run-experiment.yml"

    @classmethod
    def load(cls, path: str) -> "RemoteConfig":
        """Load configuration from a JSON file."""
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Remote config file not found: {path}")

        with open(p) as f:
            data = json.load(f)

        machines = [MachineConfig(**m) for m in data.get("machines", [])]
        wandb_data = data.get("wandb", {})
        wandb = WandBConfig(**wandb_data) if wandb_data else WandBConfig()

        return cls(
            machines=machines,
            wandb=wandb,
            github_repo=data.get("github_repo", ""),
            default_workflow=data.get("default_workflow", "run-experiment.yml"),
        )

    def get_machine(self, machine_id: str) -> MachineConfig:
        for m in self.machines:
            if m.machine_id == machine_id:
                return m
        raise ValueError(f"Machine '{machine_id}' not found in config")
