"""Tests for WandBMessageBus (using mocks)."""

from unittest.mock import MagicMock, patch

import pytest

from pyturbo.scheduler.wandb_bus import WandBMessageBus


class TestWandBMessageBus:
    def test_subscribe_and_publish_in_process(self):
        """In-process subscribers should be notified on publish."""
        bus = WandBMessageBus(project="test")
        bus._wandb = MagicMock()  # Mock wandb to avoid import

        received = []
        bus.subscribe("task/T1-train", lambda payload: received.append(payload))

        bus.publish("task/T1-train", {"state": "running"})

        assert len(received) == 1
        assert received[0]["state"] == "running"

    def test_publish_calls_wandb_log(self):
        bus = WandBMessageBus(project="test")
        mock_wandb = MagicMock()
        bus._wandb = mock_wandb

        bus.publish("task/T1-train", {"state": "success", "accuracy": 0.95})

        mock_wandb.log.assert_called_once()
        logged = mock_wandb.log.call_args[0][0]
        assert logged["event_topic"] == "task/T1-train"
        assert logged["state"] == "success"
        assert logged["accuracy"] == 0.95

    def test_publish_handles_wandb_error(self):
        """publish should not raise even if wandb.log fails."""
        bus = WandBMessageBus(project="test")
        mock_wandb = MagicMock()
        mock_wandb.log.side_effect = RuntimeError("wandb error")
        bus._wandb = mock_wandb

        # Should not raise
        bus.publish("task/T1-train", {"state": "running"})

    def test_query_run_status(self):
        bus = WandBMessageBus(project="test", entity="team")
        mock_api = MagicMock()
        mock_run = MagicMock()
        mock_run.state = "finished"
        mock_run.summary = {"accuracy": 0.95}
        mock_run.name = "T1-train"
        mock_run.config = {"lr": 0.001}
        mock_api.run.return_value = mock_run
        bus._api = mock_api
        bus._wandb = MagicMock()

        result = bus.query_run_status("run_abc123")
        assert result["state"] == "finished"
        assert result["summary"]["accuracy"] == 0.95
        mock_api.run.assert_called_once_with("team/test/run_abc123")

    def test_query_run_status_failure(self):
        bus = WandBMessageBus(project="test")
        mock_api = MagicMock()
        mock_api.run.side_effect = Exception("not found")
        bus._api = mock_api
        bus._wandb = MagicMock()

        result = bus.query_run_status("nonexistent")
        assert result is None

    def test_query_runs_by_tag(self):
        bus = WandBMessageBus(project="test")
        mock_api = MagicMock()
        mock_run = MagicMock()
        mock_run.id = "run_1"
        mock_run.name = "T1-train"
        mock_run.state = "finished"
        mock_run.summary = {"accuracy": 0.95}
        mock_api.runs.return_value = [mock_run]
        bus._api = mock_api
        bus._wandb = MagicMock()

        runs = bus.query_runs_by_tag("T1")
        assert len(runs) == 1
        assert runs[0]["name"] == "T1-train"

    def test_init_run(self):
        bus = WandBMessageBus(project="test")
        mock_wandb = MagicMock()
        mock_run = MagicMock()
        mock_run.id = "new_run_123"
        mock_wandb.init.return_value = mock_run
        bus._wandb = mock_wandb

        run_id = bus.init_run("T1-train", config={"lr": 0.001}, tags=["T1"])
        assert run_id == "new_run_123"
        mock_wandb.init.assert_called_once_with(
            project="test",
            entity=None,
            name="T1-train",
            config={"lr": 0.001},
            tags=["T1"],
        )

    def test_finish_run(self):
        bus = WandBMessageBus(project="test")
        mock_wandb = MagicMock()
        bus._wandb = mock_wandb

        bus.finish_run()
        mock_wandb.finish.assert_called_once()

    def test_ensure_wandb_raises_without_import(self):
        bus = WandBMessageBus(project="test")
        with patch.dict("sys.modules", {"wandb": None}):
            with pytest.raises(ImportError, match="wandb is required"):
                bus._ensure_wandb()
