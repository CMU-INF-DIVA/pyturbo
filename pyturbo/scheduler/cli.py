"""CLI entry point for `pyturbo run`."""

from __future__ import annotations

import argparse
import logging
import sys

logger = logging.getLogger("pyturbo.scheduler")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pyturbo",
        description="PyTurbo Scheduler - DAG-based multi-GPU task orchestrator",
    )
    sub = parser.add_subparsers(dest="command")

    # `pyturbo run` subcommand
    run_parser = sub.add_parser("run", help="Run tasks from a task definition file")

    run_parser.add_argument(
        "task_def", type=str, help="Task definition JSON file path"
    )
    run_parser.add_argument(
        "-g", "--gpu-required", type=int, default=None,
        help="Override GPU count for all tasks (default: use per-task spec)",
    )
    run_parser.add_argument(
        "-b", "--blocking", action="store_true",
        help="Block and wait when resources are unavailable",
    )
    run_parser.add_argument(
        "-p", "--poll-interval", type=int, default=30,
        help="Poll interval in seconds for blocking mode (default: 30)",
    )
    run_parser.add_argument(
        "-w", "--max-wait", type=int, default=3600,
        help="Max wait time in seconds for blocking mode (default: 3600)",
    )
    run_parser.add_argument(
        "-r", "--registry-path", type=str, default=None,
        help="Registry file path (default: ~/.pyturbo/registry.json)",
    )
    run_parser.add_argument(
        "--dry-run", action="store_true",
        help="Print allocation plan without executing",
    )
    run_parser.add_argument(
        "--gpu-ids", type=str, default=None,
        help="Manually specify GPU IDs (comma-separated, e.g. 0,1,2,3)",
    )
    run_parser.add_argument(
        "-c", "--config", type=str, default=None,
        help="Experiment YAML config file (passed through to runner)",
    )
    run_parser.add_argument(
        "-m", "--mode", type=str, default=None,
        help="Execution mode: training or evaluation (passed through to runner)",
    )
    run_parser.add_argument(
        "--model-path", type=str, default=None,
        help="Model path for evaluation mode (passed through to runner)",
    )
    run_parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable verbose logging",
    )
    run_parser.add_argument(
        "--status-dir", type=str, default="status",
        help="Directory for status JSON files (default: status/)",
    )

    # V3 remote execution options
    run_parser.add_argument(
        "--remote", action="store_true",
        help="[V3] Enable remote execution mode",
    )
    run_parser.add_argument(
        "--remote-config", type=str, default=None,
        help="[V3] Remote configuration JSON file (machines, wandb, github)",
    )
    run_parser.add_argument(
        "--machine", type=str, default=None,
        help="[V3] Target machine ID for remote execution",
    )
    run_parser.add_argument(
        "--remote-method", type=str, default="gh",
        choices=["gh", "ssh"],
        help="[V3] Remote execution method: gh (GitHub Actions) or ssh (default: gh)",
    )

    return parser


def cmd_run(args: argparse.Namespace) -> int:
    """Execute the `pyturbo run` command."""
    from .allocator import ResourceAllocator
    from .monitor import NvidiaMonitor, RegistryWatcher
    from .registry import FileLockRegistry
    from .runner import DAGRunner, LocalExecutor
    from .status import FileMessageBus, StatusStore
    from .task import TaskGraph, TaskLoader

    # Load tasks
    try:
        specs = TaskLoader.load(args.task_def)
    except FileNotFoundError as e:
        logger.error("[ERROR] %s", e)
        return 2
    except Exception as e:
        logger.error("[ERROR] Failed to load task definitions: %s", e)
        return 2

    # Override gpu_required if specified
    if args.gpu_required is not None:
        for spec in specs:
            spec.gpu_required = args.gpu_required

    # Build DAG
    try:
        dag = TaskGraph(specs)
    except ValueError as e:
        logger.error("[ERROR] Invalid task graph: %s", e)
        return 2

    # Initialize components
    try:
        monitor = NvidiaMonitor()
        registry = FileLockRegistry(args.registry_path)
    except Exception as e:
        logger.error("[ERROR] Failed to initialize GPU monitor/registry: %s", e)
        return 77

    allocator = ResourceAllocator(monitor, registry)
    executor = LocalExecutor()
    status_store = StatusStore(args.status_dir)
    bus = FileMessageBus(status_store)

    # V3: remote mode overrides executor and bus
    if getattr(args, "remote", False):
        from .config import RemoteConfig
        from .remote_executor import RemoteExecutor
        from .wandb_bus import WandBMessageBus

        if not args.remote_config:
            logger.error("[ERROR] --remote requires --remote-config")
            return 2

        try:
            remote_cfg = RemoteConfig.load(args.remote_config)
        except Exception as e:
            logger.error("[ERROR] Failed to load remote config: %s", e)
            return 2

        executor = RemoteExecutor(remote_cfg, method=args.remote_method)
        bus = WandBMessageBus(
            project=remote_cfg.wandb.project,
            entity=remote_cfg.wandb.entity,
        )

    # Run DAG
    runner = DAGRunner(
        executor=executor,
        bus=bus,
        allocator=allocator,
        status_store=status_store,
        poll_interval=args.poll_interval,
        max_wait=args.max_wait,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    return runner.run(dag)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # Setup logging
    level = logging.DEBUG if getattr(args, "verbose", False) else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.command is None:
        parser.print_help()
        sys.exit(2)

    if args.command == "run":
        exit_code = cmd_run(args)
        sys.exit(exit_code)
    else:
        parser.print_help()
        sys.exit(2)


if __name__ == "__main__":
    main()
