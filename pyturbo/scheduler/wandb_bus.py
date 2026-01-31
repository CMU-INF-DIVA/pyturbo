"""V3 WandBMessageBus: publish/subscribe via Weights & Biases."""

from __future__ import annotations

import json
import logging
from typing import Any, Callable, Optional

from .status import MessageBus

logger = logging.getLogger(__name__)


class WandBMessageBus(MessageBus):
    """Message bus using WandB for cross-machine event communication.

    - publish: Log events to WandB run
    - subscribe: Poll WandB API for status changes (or use webhook callback)
    """

    def __init__(
        self,
        project: str = "dsta-stance",
        entity: Optional[str] = None,
    ):
        self._project = project
        self._entity = entity
        self._subscribers: dict[str, list[Callable]] = {}
        self._wandb = None
        self._api = None

    def _ensure_wandb(self) -> None:
        """Lazy import wandb to avoid import errors when not installed."""
        if self._wandb is None:
            try:
                import wandb
                self._wandb = wandb
            except ImportError:
                raise ImportError(
                    "wandb is required for WandBMessageBus. "
                    "Install with: pip install wandb"
                )

    def _ensure_api(self) -> None:
        """Initialize WandB API client for querying."""
        if self._api is None:
            self._ensure_wandb()
            self._api = self._wandb.Api()

    def publish(self, topic: str, payload: dict) -> None:
        """Log an event to the active WandB run."""
        self._ensure_wandb()

        # Tag the payload with topic for filtering
        log_data = {
            "event_topic": topic,
            **{k: v for k, v in payload.items() if isinstance(v, (int, float, str, bool))},
        }

        try:
            self._wandb.log(log_data)
            logger.debug("[WANDB] Published to topic=%s: %s", topic, log_data)
        except Exception as e:
            logger.warning("[WANDB] Failed to publish: %s", e)

        # Notify in-process subscribers
        for cb in self._subscribers.get(topic, []):
            cb(payload)

    def subscribe(self, topic: str, callback: Callable[[dict], None]) -> None:
        """Register an in-process callback for a topic."""
        if topic not in self._subscribers:
            self._subscribers[topic] = []
        self._subscribers[topic].append(callback)

    def query_run_status(self, run_id: str) -> Optional[dict]:
        """Query WandB API for a specific run's status."""
        self._ensure_api()

        try:
            path = f"{self._entity}/{self._project}/{run_id}" if self._entity else f"{self._project}/{run_id}"
            run = self._api.run(path)
            return {
                "state": run.state,
                "summary": dict(run.summary) if run.summary else {},
                "name": run.name,
                "config": dict(run.config) if run.config else {},
            }
        except Exception as e:
            logger.warning("[WANDB] Failed to query run %s: %s", run_id, e)
            return None

    def query_runs_by_tag(self, tag: str) -> list[dict]:
        """Query WandB API for runs with a specific tag."""
        self._ensure_api()

        try:
            project_path = f"{self._entity}/{self._project}" if self._entity else self._project
            runs = self._api.runs(
                project_path,
                filters={"tags": {"$in": [tag]}},
            )
            return [
                {
                    "id": r.id,
                    "name": r.name,
                    "state": r.state,
                    "summary": dict(r.summary) if r.summary else {},
                }
                for r in runs
            ]
        except Exception as e:
            logger.warning("[WANDB] Failed to query runs by tag %s: %s", tag, e)
            return []

    def init_run(
        self,
        name: str,
        config: Optional[dict] = None,
        tags: Optional[list[str]] = None,
    ) -> str:
        """Initialize a new WandB run. Returns the run ID."""
        self._ensure_wandb()

        run = self._wandb.init(
            project=self._project,
            entity=self._entity,
            name=name,
            config=config or {},
            tags=tags or [],
        )
        logger.info("[WANDB] Initialized run: %s (id=%s)", name, run.id)
        return run.id

    def finish_run(self) -> None:
        """Finish the current WandB run."""
        self._ensure_wandb()
        self._wandb.finish()
        logger.info("[WANDB] Run finished")
