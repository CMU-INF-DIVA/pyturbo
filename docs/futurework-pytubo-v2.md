# V2: PTB v2 — 单机多卡 DAG + GPU 动态调度

> **适用场景**: 单台机器 × N 块 GPU（如 1 台 × 8×A100），并发进程 ≤ 10

## 0. Coding Checklist

### V2 任务清单

| Task ID | 文件路径 | 操作 | 具体内容 |
|---|---|---|---|
| V2-01 | `pyturbo/scheduler/__init__.py` | **新增** | 模块 init，导出核心类 (TaskSpec, TaskGraph, DAGRunner, etc.) |
| V2-02 | `pyturbo/scheduler/task.py` | **新增** | TaskSpec dataclass (Pydantic)、TaskGraph (DAG 拓扑+cycle 检测+runnable 集合)、TaskLoader (JSON→TaskSpec 解析) |
| V2-03 | `pyturbo/scheduler/block.py` | **新增** | BlockManager：串行组队列 + 互斥检测 (同 block_group 不可并发) |
| V2-04 | `pyturbo/scheduler/monitor.py` | **新增** | NvidiaMonitor：pynvml 封装 (空闲阈值判定 + ghost 检测) + inotify watcher (registry 变更通知) |
| V2-05 | `pyturbo/scheduler/registry.py` | **新增** | FileLockRegistry：flock + JSON 读写 + stale entry PID 清理 |
| V2-06 | `pyturbo/scheduler/allocator.py` | **新增** | ResourceAllocator：整合 Monitor + Registry，输出可用 GPU ID 列表 (连续优先/最小碎片) |
| V2-07 | `pyturbo/scheduler/runner.py` | **新增** | LocalExecutor (subprocess + CUDA_VISIBLE_DEVICES + finally 释放)、ProcessHandle、DAGRunner 核心循环 |
| V2-08 | `pyturbo/scheduler/status.py` | **新增** | StatusStore (status JSON 读写)、FileMessageBus (inotify 订阅)、FailurePropagator (超限失败传播) |
| V2-09 | `pyturbo/scheduler/cli.py` | **新增** | `pyturbo run` argparse 入口 (§12.1 全部参数)、退出码处理 |
| V2-10 | `pyturbo/scheduler/dataclasses.py` | **新增** | 所有数据类集中定义：TaskSpec, GPUState, JobStatus, RegistryEntry, Handle, TaskResult |
| V2-11 | `setup.py` 或 `pyproject.toml` | **修改** | 添加 `console_scripts` entry point: `pyturbo = pyturbo.scheduler.cli:main`；添加依赖 `pynvml`, `inotify` |
| V2-12 | `tests/test_registry.py` | **新增** | FileLockRegistry 单元测试：并发写入互斥、stale entry 清理 |
| V2-13 | `tests/test_monitor.py` | **新增** | NvidiaMonitor 单元测试：Mock pynvml 返回值，阈值判定 + ghost 检测 |
| V2-14 | `tests/test_allocator.py` | **新增** | ResourceAllocator 单元测试：Mock Monitor + Registry，GPU 分配逻辑 |
| V2-15 | `tests/test_task.py` | **新增** | TaskSpec 解析 + TaskGraph 拓扑排序/cycle 检测 |
| V2-16 | `tests/test_dag_runner.py` | **新增** | DAGRunner 集成测试：Mock Executor + 真实 DAG 文件 |

### V3 任务清单

| Task ID | 文件路径 | 操作 | 具体内容 |
|---|---|---|---|
| V3-01 | `pyturbo/scheduler/remote_executor.py` | **新增** | RemoteExecutor：`gh workflow dispatch` / SSH 远程执行；RemoteHandle 封装 |
| V3-02 | `pyturbo/scheduler/wandb_bus.py` | **新增** | WandBMessageBus：`wandb.log()` 发布 + webhook callback 订阅 |
| V3-03 | `.github/workflows/run-experiment.yml` | **新增** | GitHub Actions workflow_dispatch 模板 (见 V3 §5) |
| V3-04 | `pyturbo/scheduler/runner.py` | **修改** | DAGRunner 注入 RemoteExecutor + WandBMessageBus（接口已预埋，零改动核心循环） |
| V3-05 | `pyturbo/scheduler/cli.py` | **修改** | 新增 `--remote` / `--machine` 参数，支持多机调度模式 |
| V3-06 | `pyturbo/scheduler/config.py` | **新增** | 多机配置加载：machine_id → hostname/GPU 数量映射、WandB project 配置 |
| V3-07 | `tests/test_remote_executor.py` | **新增** | RemoteExecutor Mock 测试 |
| V3-08 | `tests/test_wandb_bus.py` | **新增** | WandBMessageBus Mock 测试 |

## 1. 目标

| 目标 | 说明 | 交付物/落地形式 |
|---|---|---|
| DAG 依赖与阻塞调度 | 任务依赖显式建图, 支持串行组与互斥检测, 自动解锁后继 | TaskGraph + BlockManager + Scheduler |
| GPU 动态监控与分配 | 任务声明 gpu_required, 按实时空闲 GPU 分配 | GPUStateMonitor + Allocator |
| job-gpu 注册表 + double check | 防 ghost 占用, 资源分配强一致 | GPURegistry + Monitor 双校验 |
| Job 状态表与失败传播 | 状态持久化, 超限失败传播到分支上下游 | StatusStore + FailurePropagate |
| 兼容 eval 分块与 train 多卡 | eval 可分块, train 多卡协同执行 | Executor + runner 参数 |

## 2. 模块划分

| 模块 | 职责 | 核心输入 | 核心输出 | 关键细节 |
|---|---|---|---|---|
| TaskSpec/JobSpec | 任务描述 | Cloud Code JSON | 结构化 TaskSpec | 解析 blockedBy/blocks/description |
| TaskGraph | 依赖建图 | TaskSpec | runnable 任务集 | cycle 检测, 拓扑可运行 |
| BlockManager | 串行/互斥 | block_group | 可调度任务 | 同组队列, 互斥锁 |
| GPUStateMonitor | GPU 实时状态 | pynvml | GPUState | 进程列表+显存阈值 |
| GPURegistry | 占用注册表 | job/gpu 状态 | 占用视图 | 写入/释放/锁 |
| Allocator | GPU 分配 | GPUState + 需求 | gpu_ids | 连续优先, 最小碎片 |
| Scheduler | 调度入口 | runnable + 资源 | dispatch | 生产者逻辑 |
| Executor | 任务执行 | command + gpu_ids | 进程结果 | CUDA_VISIBLE_DEVICES |
| StatusStore | 状态持久化 | 执行结果 | JobStatus | status JSON |

## 3. 数据类

| 数据类 | 字段 | 说明 |
|---|---|---|
| TaskSpec | id, subject, description, activeForm, deps, blocks, status, gpu_required, block_group, max_retry, command, status_output, tags, priority | 任务最小原子 |
| GPUState | id, mem_used, mem_total, util, processes, available | 实时 GPU 状态 |
| JobStatus | state, start_time, end_time, gpu_ids, exit_code, retry_count, artifacts, wandb_run_id | 任务运行状态 |
| RegistryEntry | job_id, gpu_ids, state, lock_ts | 注册表条目 |
| TaskGraph | nodes, edges_in, edges_out | DAG 结构 |

## 4. Pipeline 设计

| 阶段 | 组件 | 输入 | 输出 | 说明 |
|---|---|---|---|---|
| 任务层 | TaskLoader | Cloud Code JSON | TaskSpec 列表 | 兼容 blockedBy/blocks |
| 依赖层 | TaskGraph | TaskSpec | runnable 集合 | 拓扑可运行判定 |
| 资源层 | GPU Monitor + Registry | GPU 状态 | 可用 GPU 列表 | 双重检查 |
| 调度层 | Scheduler | runnable + GPU | dispatch 任务 | 生产者 |
| 执行层 | Executor | command + gpu_ids | 进程结果 | 消费者 |
| 状态层 | StatusStore | 进程结果 | JobStatus | status JSON |

## 5. 调度算法

| 步骤 | 操作 | 判定 | 输出 |
|---|---|---|---|
| 1 | MonitorGPU | 读取 GPU 实时信息 | GPUState |
| 2 | RegistryCheck | 读取占用注册表 | GPURegistry |
| 3 | RunnableCheck | deps 全部 success | runnable jobs |
| 4 | BlockCheck | 同组串行+互斥检测 | 过滤 runnable |
| 5 | AllocateGPU | 满足 gpu_required | job->gpu_ids |
| 6 | DispatchJob | 启动 runner | process |
| 7 | UpdateStatus | 写 status JSON | JobStatus |
| 8 | FailurePropagate | 超限失败 | 更新 DAG |

## 6. 关键规则

| 规则 | 细节 | 示例 |
|---|---|---|
| GPU 空闲判定 | Registry 空闲 && Monitor 空闲 | mem_used < 阈值 且无进程 |
| 命令解析 | 优先 ```bash 代码块 | 多代码块取第一段 |
| 失败传播 | 超过 max_retry -> 前序+后续失败 | 停止分支 |
| 并发一致性 | Registry 与 Monitor 不一致拒绝分配 | ghost 保护 |

## 7. 任务 JSON 扩展字段

| 字段 | 说明 | 备注 |
|---|---|---|
| gpu_required | 任务所需 GPU 数量 | 支持 1/2/3/4... |
| block_group | 串行组 ID | 同组顺序执行 |
| max_retry | 最大重试次数 | 超限失败传播 |
| status_output | 状态文件路径 | status/<task_id>.json |
| command | 直接执行命令 | 优先于 description 解析 |
| tags | 标签 | 过滤/统计 |
| priority | 优先级 | 可选扩展 |

## 8. 状态文件 JSON 模板

```json
{
  "job_id": "T1-task3",
  "state": "running",
  "start_time": "2026-01-31T15:08:00",
  "end_time": null,
  "gpu_ids": [0, 1, 2, 3],
  "exit_code": null,
  "retry_count": 0,
  "artifacts": {
    "output_dir": "output/t1_tiktok_qwen_clf_mp",
    "metrics_path": "output/t1_tiktok_qwen_clf_mp/metrics.json"
  },
  "wandb_run_id": "run_abc123"
}
```

## 9. 图示

### 9.1 总体流程图

```
Task JSON -> TaskLoader -> TaskGraph -> Scheduler
                                  |         |
                             GPU Monitor   Executor
                                  |         |
                              GPURegistry  StatusStore
                                  \_________/
                                      WandB
```

### 9.2 状态机图

```
pending -> runnable -> running -> success
    |         |           |
    v         v           v
  blocked   failed <-> retry
              |
              v
         branch_fail
```

### 9.3 并发一致性序列图

```
Monitor -> GPUState
Registry -> GPURegistry
Allocator: intersect(GPUState, GPURegistry)
Executor -> Registry: lock(gpu_ids)
Job done -> Registry: release(gpu_ids)
```

## 10. 架构: Daemon-less + FileLock

| 特性 | 原计划 (Daemon) | **当前决策 (Daemon-less + FileLock)** |
|:---|:---|:---|
|  **进程模型**  | 常驻后台服务 (Service) | **DAG Runner CLI (`pyturbo run`)** |
|  **并发控制**  | Daemon 内部内存队列 | **操作系统文件锁 (`fcntl` / `.lock`)** |
|  **状态维护**  | 内存 + 异步刷盘 | **直接读写共享状态文件** |
|  **故障恢复**  | Daemon 崩溃需重启恢复 | **进程崩溃即释放锁，天然容错** |
|  **运维成本**  | 高 (需管理 Service 生命周期) | **极低 (无状态，随 Runner 启动)** |

> **参考来源**: Registry 和 Executor 的设计模式参考了 `tmp_pyturbo-v2/video-ui-platform/` 中 `backend/routes/workers.js` 的 Worker 生命周期管理实现（GPU 隔离 via `CUDA_VISIBLE_DEVICES`、PID 追踪 via `workerProcesses` Map、`finally` 块资源释放）。PTB v2 将这些模式从 Redis + Node.js 转换为 Python + 文件锁实现。

### 10.1 关键组件实现落地

1.  **Registry (注册表)**:
    *   通过 `flock` 锁定的 JSON 文件。
    *   包含当前活跃的 Job 列表及其占用的 GPU ID。
    *   **PID 追踪**（复用自 video-ui-platform）：每个 Registry Entry 记录执行进程的 `pid`，用于 stale entry 检测和清理。
2.  **Monitor (物理双检)**:
    *   在获取 Registry 锁后，**必须** 调用 `pynvml` 检查目标 GPU 的真实物理占用 (显存/利用率)。
    *   **Ghost Protection**: 如果 Registry 显示空闲，但 Monitor 显示物理被占，则标记为不可用并跳过。
3.  **Executor (任务执行)**:
    *   **GPU 隔离**（复用自 video-ui-platform）：通过 `subprocess.Popen(env={"CUDA_VISIBLE_DEVICES": gpu_ids})` 注入环境变量。
    *   **资源释放保证**（复用自 video-ui-platform）：在 `try/finally` 块中确保任务结束后必定释放 Registry 占位。

### 10.2 "task" 调用图

```
GitHub Action / User
       |
       v
  pyturbo run (CLI)
       |
       +---> [1. Acquire FileLock] <---+
       |                               |
       +---> [2. Read Registry]        | (Spin Wait)
       |                               |
       +---> [3. Monitor GPU] ---------+
       |           |
       |     (Resource OK)
       |           |
       v           v
  [4. Write Registry (Occupied)]
       |
       v
  [5. Execute Job (subprocess)]
       |
       v
  [6. Release Registry]
```

## 11. 代码目录结构

```text
pyturbo/
├── __init__.py
├── pipeline.py
├── resource.py
├── runtime.py
├── stage.py
├── system.py
├── task.py
├── utils.py
├── worker.py
└── scheduler/               # [v2] 新增调度器模块
    ├── __init__.py
    ├── cli.py               # pyturbo run 入口 (argparse + 退出码)
    ├── dataclasses.py       # 所有数据类 (TaskSpec, GPUState, JobStatus, etc.)
    ├── task.py              # TaskLoader + TaskGraph (DAG 拓扑)
    ├── block.py             # BlockManager (串行组 + 互斥检测)
    ├── monitor.py           # NvidiaMonitor (pynvml + inotify watcher)
    ├── registry.py          # FileLockRegistry (flock + stale PID 清理)
    ├── allocator.py         # ResourceAllocator (Monitor + Registry → 可用 GPU)
    ├── runner.py            # LocalExecutor + DAGRunner 核心循环
    └── status.py            # StatusStore + FileMessageBus + FailurePropagator
```

## 12. 实施规格

### 12.1 `pyturbo run` CLI 参数定义

| 参数 | 短名 | 类型 | 默认值 | 必填 | 说明 |
|---|---|---|---|---|---|
| `task_def` | (位置参数) | `str` | - | 是 | 任务定义 JSON 文件路径 |
| `--gpu-required` | `-g` | `int` | `4` | 否 | 任务所需 GPU 数量 |
| `--blocking` | `-b` | `flag` | `False` | 否 | 阻塞模式：资源不足时自旋等待而非立即退出 |
| `--poll-interval` | `-p` | `int` (秒) | `30` | 否 | 自旋等待轮询间隔（仅 `--blocking` 时生效） |
| `--max-wait` | `-w` | `int` (秒) | `3600` | 否 | 最大等待时间，超时退出码 75（仅 `--blocking` 时生效） |
| `--registry-path` | `-r` | `str` | `~/.pyturbo/registry.json` | 否 | Registry 文件路径 |
| `--dry-run` | | `flag` | `False` | 否 | 仅打印分配计划，不实际执行任务 |
| `--gpu-ids` | | `str` | (自动分配) | 否 | 手动指定 GPU ID，逗号分隔（如 `0,1,2,3`），跳过自动分配 |
| `--config` | `-c` | `str` | - | 否 | 实验 YAML 配置文件路径（透传给 runner） |
| `--mode` | `-m` | `str` | - | 否 | 执行模式（`training` / `evaluation`），透传给 runner |
| `--model-path` | | `str` | - | 否 | 评估模式下的模型路径，透传给 runner |
| `--verbose` | `-v` | `flag` | `False` | 否 | 详细日志输出 |

#### 退出码约定

| 退出码 | 含义 |
|---|---|
| `0` | 任务成功完成 |
| `1` | 任务执行失败（runner 返回非零） |
| `2` | 参数错误 / 配置文件不存在 |
| `75` | 资源等待超时（`--blocking` + `--max-wait` 超限） |
| `77` | Registry 锁获取失败（不可恢复的文件锁错误） |

#### 自旋等待策略（方案 A+B 混合，见 QA-8 决策）

*   **基础间隔**: `--poll-interval` 秒（默认 30s）作为 fallback 上限
*   **inotify 优化**: 使用 `inotify` 监听 `registry.json` 的 `IN_MODIFY` 事件；当其他进程释放 GPU 并更新 registry 时，等待进程在 ~100ms 内被唤醒并重新尝试分配
*   **混合逻辑**: `inotify.event_gen(timeout_s=poll_interval)` — 收到文件变更事件则立即重试，超时则按 poll_interval fallback 轮询
*   **thundering herd**: 多个等待者同时唤醒，但 `flock` 保证只有一个获得锁并成功分配；并发 ≤ 10 时开销可忽略
*   **退避策略**: 无指数退避（GPU 释放是离散事件，退避无意义）
*   **超时**: `--max-wait` 秒后退出，退出码 75
*   **日志**: 每次轮询打印一行 `[WAIT] poll #{n}, elapsed={t}s, available_gpus={k}/{total}`

### 12.2 Registry 文件规范

#### 文件路径

*   默认位置: `~/.pyturbo/registry.json`
*   可通过 `--registry-path` 覆盖
*   锁文件: 与 registry 同目录，名为 `registry.lock`（`flock` 使用）
*   首次运行时自动创建目录和空 registry

#### JSON Schema（顶层结构）

```json
{
  "version": 1,
  "updated_at": "2026-01-31T15:08:00",
  "jobs": [
    {
      "job_id": "T1-train",
      "gpu_ids": [0, 1, 2, 3],
      "pid": 12345,
      "started_at": "2026-01-31T15:08:00",
      "command": "scripts/run_test_train_model.sh t1 0,1,2,3"
    }
  ]
}
```

#### 字段说明

| 字段 | 类型 | 说明 |
|---|---|---|
| `version` | `int` | Schema 版本号，当前固定为 `1` |
| `updated_at` | `str` (ISO8601) | 最后一次写入时间 |
| `jobs` | `list[RegistryEntry]` | 当前活跃任务列表 |
| `jobs[].job_id` | `str` | 任务 ID（来自 task_def 或自动生成） |
| `jobs[].gpu_ids` | `list[int]` | 占用的 GPU ID 列表 |
| `jobs[].pid` | `int` | 执行进程的 PID |
| `jobs[].started_at` | `str` (ISO8601) | 任务开始时间 |
| `jobs[].command` | `str` | 执行的完整命令（用于调试） |

#### Stale Entry 清理策略

> **模式来源**: video-ui-platform 使用 Redis heartbeat 超时（30s）判定 Worker 是否存活。PTB v2 将此概念简化为 **PID 存活检查**，无需心跳机制。

1.  **获取 `flock` 锁后**，遍历 `jobs` 列表
2.  对每个 entry，检查 `pid` 是否存活：`os.kill(pid, 0)` 或 `/proc/{pid}/status`
3.  如果进程已不存在 → **stale entry** → 从 `jobs` 中移除并写回
4.  日志输出: `[CLEANUP] Removed stale entry job_id={id}, pid={pid} (process dead)`
5.  `flock` 本身在进程崩溃时自动释放，因此锁文件不需要额外清理

### 12.3 GPU Monitor 判定规范

统一使用 **`pynvml`**（NVIDIA Management Library Python 绑定），不使用 `nvidia-smi` 子进程。

#### 空闲判定阈值

| 指标 | 阈值 | 说明 |
|---|---|---|
| 显存占用 (mem_used) | < 200 MB | 空 GPU 通常有 ~50MB 系统占用 |
| GPU 利用率 (utilization) | < 5% | 排除偶发波动 |
| 计算进程数 | = 0 | `nvmlDeviceGetComputeRunningProcesses` 返回空 |

**三项全部满足 → GPU 判定为空闲**

#### Ghost 进程检测逻辑

1.  Registry 显示 GPU X 空闲（无 job 占用）
2.  Monitor 检测到 GPU X 物理被占（显存 > 200MB 或有计算进程）
3.  → 标记为 **ghost occupied**，该 GPU 不可分配
4.  日志: `[GHOST] GPU {id} physically occupied but not in registry (mem={used}MB, procs={n})`

#### inotify Registry Watcher（QA-8 方案 B 落地）

Monitor 模块同时提供 inotify watcher，用于自旋等待时的低延迟唤醒：

```python
import inotify.adapters

class RegistryWatcher:
    """监听 registry.json 变更，用于替代固定间隔 sleep。"""

    def __init__(self, registry_path: str):
        self._inotify = inotify.adapters.Inotify()
        self._inotify.add_watch(str(Path(registry_path).parent))
        self._registry_name = Path(registry_path).name

    def wait_for_change(self, timeout_s: int) -> bool:
        """阻塞等待 registry 文件变更，返回是否有变更。"""
        for event in self._inotify.event_gen(timeout_s=timeout_s):
            if event is None:
                return False  # timeout, fallback to poll
            _, type_names, _, filename = event
            if filename == self._registry_name and 'IN_MODIFY' in type_names:
                return True  # registry changed, wake up immediately
        return False
```

*   **依赖**: `inotify` PyPI 包（Linux only，`pip install inotify`）
*   **降级**: 如果 inotify 不可用（如非 Linux 平台），退化为纯 `time.sleep(poll_interval)`

### 12.4 BlockManager 实施规格

BlockManager 负责串行组调度和互斥检测（见 §2 模块表、QA-需求对齐）。

#### 串行组（block_group）

*   同一 `block_group` 内的任务按 `priority` 排序，形成 FIFO 队列
*   同组内同一时间只允许一个任务处于 `running` 状态
*   当前 running 任务完成后，下一个任务才可进入 `runnable`

#### 互斥检测

*   不同 `block_group` 的任务可以并行（GPU 资源允许的前提下）
*   DAG 依赖（`blockedBy`）优先于 block_group 串行约束

#### 接口

```python
class BlockManager:
    def __init__(self, specs: list[TaskSpec]):
        # 按 block_group 分组，每组内按 priority 排序
        self._groups: dict[str, deque[str]] = ...

    def is_runnable(self, task_id: str, dag: TaskGraph) -> bool:
        """判断任务是否可运行：DAG 依赖满足 且 同组无 running 任务。"""
        spec = dag.get_spec(task_id)
        if not dag.deps_satisfied(task_id):
            return False
        if spec.block_group:
            group_running = [t for t in self._groups[spec.block_group]
                           if dag.get_status(t) == "running"]
            return len(group_running) == 0
        return True
```

### 12.5 FailurePropagator 实施规格

任务失败超过 `max_retry` 后，传播到 DAG 上下游：

1.  **重试**: `exit_code != 0` 且 `retry_count < max_retry` → 重新加入 runnable 队列
2.  **超限失败**: `retry_count >= max_retry` → 状态设为 `failed`
3.  **传播**: 所有 `blockedBy` 包含失败任务的下游任务 → 状态设为 `branch_fail`
4.  **日志**: `[FAIL] task={id} exceeded max_retry={n}, propagating failure to {downstream_ids}`

### 12.6 真实 Runner 脚本参考

PyTurbo v2 的 Executor 最终调用的是 `dsta-stance-clean-refactor-s2` 项目中的 runner 脚本。

**项目路径**: `/home/wenhel/dsta-stance-project/dsta-stance-clean-refactor-s2`

#### 可用脚本清单

| 脚本路径 | 用途 | 接口 | GPU 需求 |
|---|---|---|---|
| `scripts/run_test_train_model.sh` | T1-T9 训练 | `{script} {exp_id} {gpu_ids}` | 是 |
| `scripts/run_test_evaltarget_model-local.sh` | T1-T9 本地评估（script 模式） | `{script} {exp_id} {gpu_ids}` | 是 |
| `scripts/run_test_evaltarget_model-deliver.sh` | T1-T9 交付评估（pipeline 模式+dryrun） | `{script} {exp_id} {gpu_ids} [--dryrun]` | 是 |
| `scripts/run_test_evaltarget_model-deliver-ppl.sh` | T1-T9 交付评估（pipeline 并行+4GPU） | `{script} {exp_id} {gpu_ids} [--dryrun]` | 是 (4 GPU) |
| `scripts/run_test_api_eval.sh` | API 评估（Gemini/Claude/GPT） | `{script} {exp_id} [--remove-cache] [--limit N]` | 否 |
| `scripts/run_test_labeler.sh` | Labeler 测试 | `{script} {labeler} {dataset}` | 部分需要 |

#### 训练脚本核心调用模式

```bash
CUDA_VISIBLE_DEVICES=$GPU_IDS python -m stance.core.runner \
    --mode training \
    --system script \
    --config "$config" \
    --gpu-ids "$GPU_IDS"
```

#### 评估脚本核心调用模式

```bash
# script 模式（单进程）
CUDA_VISIBLE_DEVICES=$GPU_IDS python -m stance.core.runner \
    --mode evaluation --system script \
    --config "$config" --model-path "$model_path" --gpu-ids "$GPU_IDS"

# pipeline 模式（多 GPU 并行）
CUDA_VISIBLE_DEVICES=$GPU_IDS python -m stance.core.runner \
    --mode evaluation --system pipeline --parallel --num-gpus 4 \
    --config "$config" --model-path "$model_path" --gpu-ids "$GPU_IDS"
```

#### Executor 调用方式

```python
command = f"bash {PROJECT_ROOT}/scripts/run_test_train_model.sh {exp_id} {gpu_ids}"
env = {"CUDA_VISIBLE_DEVICES": gpu_ids}
proc = subprocess.Popen(command, shell=True, env=env)
```

### 12.7 task_def.json 完整示例

<details>
<summary>单任务示例</summary>

```json
{
  "id": "T1-train",
  "subject": "Train T1 TikTok Qwen-Clf",
  "description": "使用 run_test_train_model.sh 训练 T1 实验",
  "activeForm": "Training T1 TikTok Qwen-Clf",
  "status": "pending",
  "gpu_required": 4,
  "block_group": "gpu-group-1",
  "max_retry": 1,
  "command": "bash /home/wenhel/dsta-stance-project/dsta-stance-clean-refactor-s2/scripts/run_test_train_model.sh t1 {gpu_ids}",
  "tags": ["train", "t1", "qwen-clf"],
  "priority": 1,
  "blocks": ["T1-eval"],
  "blockedBy": []
}
```

</details>

<details>
<summary>多任务 DAG 示例（T1/T3 训练+评估链）</summary>

```json
[
  {
    "id": "T1-train",
    "subject": "Train T1 TikTok Qwen-Clf",
    "description": "T1 训练: TikTok Qwen-Clf, 目标 76.04%",
    "activeForm": "Training T1",
    "status": "pending",
    "gpu_required": 4,
    "block_group": "gpu-group-1",
    "max_retry": 1,
    "command": "bash /home/wenhel/dsta-stance-project/dsta-stance-clean-refactor-s2/scripts/run_test_train_model.sh t1 {gpu_ids}",
    "tags": ["train", "t1"],
    "priority": 1,
    "blocks": ["T1-eval"],
    "blockedBy": []
  },
  {
    "id": "T1-eval",
    "subject": "Evaluate T1 TikTok Qwen-Clf",
    "description": "T1 评估: 使用训练产出的 best_model, 目标 75.00%",
    "activeForm": "Evaluating T1",
    "status": "pending",
    "gpu_required": 4,
    "block_group": "gpu-group-1",
    "max_retry": 1,
    "command": "bash /home/wenhel/dsta-stance-project/dsta-stance-clean-refactor-s2/scripts/run_test_evaltarget_model-deliver-ppl.sh t1 {gpu_ids}",
    "tags": ["eval", "t1"],
    "priority": 2,
    "blocks": [],
    "blockedBy": ["T1-train"]
  },
  {
    "id": "T3-train",
    "subject": "Train T3 Climate Qwen-Gen",
    "description": "T3 训练: Climate Qwen-Gen, 目标 71.43%",
    "activeForm": "Training T3",
    "status": "pending",
    "gpu_required": 4,
    "block_group": "gpu-group-2",
    "max_retry": 1,
    "command": "bash /home/wenhel/dsta-stance-project/dsta-stance-clean-refactor-s2/scripts/run_test_train_model.sh t3 {gpu_ids}",
    "tags": ["train", "t3"],
    "priority": 1,
    "blocks": ["T3-eval"],
    "blockedBy": []
  },
  {
    "id": "T3-eval",
    "subject": "Evaluate T3 Climate Qwen-Gen",
    "description": "T3 评估: 使用训练产出的 lora_model, 目标 70.71%",
    "activeForm": "Evaluating T3",
    "status": "pending",
    "gpu_required": 4,
    "block_group": "gpu-group-2",
    "max_retry": 1,
    "command": "bash /home/wenhel/dsta-stance-project/dsta-stance-clean-refactor-s2/scripts/run_test_evaltarget_model-deliver-ppl.sh t3 {gpu_ids}",
    "tags": ["eval", "t3"],
    "priority": 2,
    "blocks": [],
    "blockedBy": ["T3-train"]
  }
]
```

**说明**: `{gpu_ids}` 是占位符，由 Executor 在分配 GPU 后替换为实际值（如 `0,1,2,3`）。

</details>

### 12.8 Claude Code Task JSON 格式参考

<details>
<summary>目录结构与字段映射</summary>

```text
tmp_pyturbo-v2/tasks/
├── c6e51f87-fb8e-4cbd-9749-848574c18977/    # UUID 分组（T1-T6 实验）
│   ├── 1.json - 24.json
│   └── (no .lock)
├── dsta-stance-...-refactor-stage2-dev/       # 开发任务分组
│   ├── 1.json - 22.json
│   └── .lock
└── dsta-stance-...-dev-glm/                   # GLM 变体分组
    ├── 1.json - 2.json
    └── .lock
```

| Claude Code 字段 | PTB v2 TaskSpec 字段 | 转换说明 |
|---|---|---|
| `id` | `id` | 直接映射 |
| `subject` | `subject` | 直接映射 |
| `description` | `description` + `command` | 需从 description 中提取 `command`（解析 ```bash 代码块） |
| `activeForm` | `activeForm` | 直接映射 |
| `status` | `status` | 直接映射 |
| `blocks` | `blocks` | 直接映射 |
| `blockedBy` | `deps` | 直接映射 |
| (无) | `gpu_required` | 需从 description 中提取或在 task_def 中显式声明 |
| (无) | `block_group` | 需额外声明 |
| (无) | `max_retry` | 需额外声明，默认 1 |

</details>

### 12.9 v2 预埋的抽象接口（为 v3 扩展）

```python
# === 抽象接口 (v2/v3 共用) ===

class Executor(ABC):
    def run(self, machine_id: str, spec: TaskSpec, gpu_ids: list[int]) -> Handle: ...
    def wait(self, handle: Handle) -> TaskResult: ...

class MessageBus(ABC):
    def publish(self, topic: str, payload: dict): ...
    def subscribe(self, topic: str, callback: Callable): ...

# === v2 实现: 本地进程 + 文件通知 ===

class LocalExecutor(Executor):
    def run(self, machine_id, spec, gpu_ids):
        env = {"CUDA_VISIBLE_DEVICES": ",".join(map(str, gpu_ids))}
        proc = subprocess.Popen(spec.command, env=env, shell=True)
        return ProcessHandle(proc, spec.id)

    def wait(self, handle):
        exit_code = handle.proc.wait()
        return TaskResult(task_id=handle.task_id, exit_code=exit_code)

class FileMessageBus(MessageBus):
    def publish(self, topic, payload):
        write_json(f"status/{topic}.json", payload)

    def subscribe(self, topic, callback):
        inotify_watch(f"status/{topic}.json", callback)
```

### 12.10 DAG Runner 核心循环

```python
class DAGRunner:
    """pyturbo run 的核心逻辑，v2/v3 通用。"""

    def __init__(self, executor: Executor, bus: MessageBus, allocator: ResourceAllocator):
        self.executor = executor
        self.bus = bus
        self.allocator = allocator

    def run(self, dag: TaskGraph):
        active: dict[str, Handle] = {}  # task_id → handle

        while not dag.all_done():
            # 1. 找出可运行的任务
            runnable = dag.get_runnable()

            # 2. 尝试为每个可运行任务分配 GPU
            for spec in runnable:
                gpu_ids = self.allocator.try_acquire(spec.gpu_required)
                if gpu_ids:
                    handle = self.executor.run("localhost", spec, gpu_ids)
                    active[spec.id] = handle
                    dag.set_status(spec.id, "running")
                    self.bus.publish(f"task/{spec.id}", {"state": "running"})

            # 3. 等待任意一个活跃任务完成
            done_id, result = self.wait_any(active)
            active.pop(done_id)

            # 4. 更新状态，释放 GPU，解锁下游
            self.allocator.release(result.gpu_ids)
            dag.set_status(done_id, "success" if result.exit_code == 0 else "failed")
            self.bus.publish(f"task/{done_id}", {"state": dag.get_status(done_id)})

            # 5. 失败传播
            if result.exit_code != 0:
                dag.propagate_failure(done_id)
```

### 12.11 测试策略

#### Phase 1: 单元测试（无 GPU 环境可运行）

| 测试对象 | 测试方法 | 说明 |
|---|---|---|
| `FileLockRegistry` | Mock 文件系统 + 多进程 fork | 验证并发写入互斥、stale entry 清理 |
| `NvidiaMonitor` | Mock `pynvml` 返回值 | 验证阈值判定逻辑（空闲/占用/ghost） |
| `ResourceAllocator` | 注入 MockMonitor + MockRegistry | 验证 GPU 分配逻辑（连续优先、最小碎片） |
| `TaskSpec` 解析 | 静态 JSON 文件 | 验证字段解析、默认值、校验 |
| `TaskGraph` | 静态依赖图 | 验证拓扑排序、cycle 检测、runnable 集合 |

#### Phase 2: 集成测试（需要 GPU）

| 测试场景 | 方法 | 预期结果 |
|---|---|---|
| 单任务执行 | `pyturbo run --dry-run task_def.json` | 打印分配计划，不实际执行 |
| 并发竞争 | 同时启动 2 个 `pyturbo run --blocking` | 一个立即执行，一个等待 |
| 进程崩溃恢复 | Kill 执行中的进程 | Registry 中 stale entry 被下次轮询清理 |
| Ghost 检测 | 手动在 GPU 上跑一个 dummy 进程 | Monitor 检测到 ghost，拒绝分配 |

#### Phase 3: 端到端测试

*   使用 T1-train → T1-eval 的 DAG，验证依赖链路全流程
*   目标: `pyturbo run --blocking dag_tasks.json` 自动完成训练→评估

## 13. 实施计划 (Implementation Roadmap)

1.  **Phase 1: Foundation**
    *   实现 `FileLockRegistry` (文件锁互斥)。
    *   实现 `NvidiaMonitor` (物理资源检查)。
    *   实现 `ResourceAllocator` (整合上述两者，输出可用 GPU ID)。
2.  **Phase 2: Task Model**
    *   定义 `TaskSpec` 数据结构 (Pydantic)。
    *   实现 `TaskGraph` 解析与 `blockedBy` 依赖计算。
3.  **Phase 3: CLI & Integration**
    *   实现 `pyturbo run` 命令行入口。
    *   在 Demo 环境下验证多进程并发竞争资源的正确性。

---

# V3: VPS-GPU Runner 管理方案集成

> **适用场景**: 多台机器 × 每台多 GPU（CN VPS 编码 + 远端 GPU 执行），详见 `VPS-GPU_runner-management.md`

## 1. 对接层

| 对接层 | 说明 | 产物 |
|---|---|---|
| 任务触发层 | Webhook/本地 hook 触发 PTB | TaskTriggered 事件 |
| 调度层 | PTB 作为中间层分配 GPU/调度任务 | TaskScheduled + gpu_ids |
| 执行层 | PTB 调用 runner 执行任务 | 进程结果 + status |
| 事实中枢层 | WandB 统一记录实验状态 | WandB metrics/artifacts |

## 2. 触发链路

| 链路 | 说明 |
|---|---|
| Webhook/本地 hook -> PTB | 外部事件触发 PTB, PTB 接收任务参数 |
| PTB -> Runner | PTB 分配 GPU 后直接启动 runner |
| Runner -> WandB | runner 上报指标与 artifacts |
| PTB <- WandB | PTB 读取 WandB 状态作为完成判定 |

## 3. Gatekeeper Pattern

在 GitHub Actions Runner 环境中，PyTurbo 扮演 **资源守门员 (Resource Gatekeeper)** 的角色。

*   **触发流**: GitHub Actions 并发触发 N 个 Workflow -> N 个 Runner 几乎同时启动。
*   **执行流**:
    ```bash
    pyturbo run --blocking --gpu-required 2 task_def.json
    ```
*   **排队逻辑**:
    1.  进程启动 -> 获取 Registry Lock。
    2.  检查资源 -> 资源不足 -> 释放 Lock -> Sleep N 秒 -> 重试 (自旋等待)。
    3.  资源满足 -> 写入 Registry (占位) -> 释放 Registry Lock -> **执行任务**。
    4.  任务结束 -> 获取 Registry Lock -> 清除占位 -> 释放 Lock。

## 4. v3 Executor / MessageBus 实现

v3 只需替换 v2 预埋的抽象接口（见 V2 §12.9），DAG Runner 核心循环零改动：

```python
# v2 → v3 变化点 1: Executor
v2:  LocalExecutor   →  subprocess.Popen(command, env=CUDA...)
v3:  RemoteExecutor  →  gh workflow dispatch / SSH remote exec

# v2 → v3 变化点 2: MessageBus
v2:  FileMessageBus  →  write status.json + inotify
v3:  WandBMessageBus →  wandb.log() + webhook callback
```

```python
class RemoteExecutor(Executor):
    def run(self, machine_id, spec, gpu_ids):
        run_id = gh_dispatch(machine_id, spec, gpu_ids)
        return RemoteHandle(run_id, spec.id)

    def wait(self, handle):
        return poll_wandb_status(handle.run_id)

class WandBMessageBus(MessageBus):
    def publish(self, topic, payload):
        wandb.log(payload)

    def subscribe(self, topic, callback):
        webhook_server.register(topic, callback)
```

## 5. GitHub Actions Workflow 模板

```yaml
# .github/workflows/run-experiment.yml
name: Run GPU Experiment
on:
  workflow_dispatch:
    inputs:
      experiment:
        description: 'Experiment ID (e.g. t1, t3, all)'
        required: true
      mode:
        description: 'Mode: training or evaluation'
        required: true
        default: 'training'
      gpu_count:
        description: 'Number of GPUs required'
        required: true
        default: '4'

jobs:
  run:
    runs-on: [self-hosted, gpu]
    steps:
      - uses: actions/checkout@v4
      - name: Run experiment via PyTurbo
        run: |
          pyturbo run \
            --blocking \
            --gpu-required ${{ github.event.inputs.gpu_count }} \
            --mode ${{ github.event.inputs.mode }} \
            --poll-interval 30 \
            --max-wait 7200 \
            task_defs/${{ github.event.inputs.experiment }}.json
```

## 6. WandB 集成细节

### Runner 端上报

```python
import wandb

run = wandb.init(
    project="dsta-stance",
    name=f"{exp_id}-{mode}",
    config={"experiment": exp_id, "gpu_ids": gpu_ids, "mode": mode},
    tags=[exp_id, mode]
)
wandb.log({"accuracy": acc, "loss": loss, "epoch": epoch})
wandb.finish()
```

### VPS 端查询

```python
import wandb
api = wandb.Api()
runs = api.runs("dsta-stance", filters={"tags": {"$in": ["T1"]}})
for r in runs:
    print(r.name, r.state, r.summary.get("accuracy"))
```

### WandB Automations Webhook 配置

*   触发条件: Run 状态变为 `finished`
*   Payload: `{"run_id": "<run_id>", "project": "dsta-stance", "state": "finished"}`
*   目标: GitHub Actions `workflow_dispatch` API 或自建 Webhook SRV

---

# V3b: Monitoring Web UI（暂不实现）

> **状态**: 仅设计讨论阶段，等 V2/V3 核心调度功能稳定后再实现。
>
> **需求**: 提供一个网页界面，实时显示 GPU 使用进度和 Pipeline（DAG）执行进度。

## 1. 需求范围

| 视图 | 数据来源 | 展示内容 |
|---|---|---|
| GPU 使用面板 | `registry.json` + pynvml | 各 GPU 占用率、显存、当前绑定的任务 ID |
| Pipeline 进度面板 | `status/<task_id>.json` + DAG 定义 | DAG 拓扑图 + 各节点状态（pending/running/done/failed） |

## 2. 方案对比

| | 方案 A: 复用 video-ui-platform | 方案 B: 轻量 Python Web | 方案 C: 静态 HTML 生成 |
|---|---|---|---|
| **前端** | React 19 + Vite | 简单 HTML + JS 轮询 | 纯静态 HTML（DAGRunner 每轮刷新生成） |
| **后端** | Express.js + Redis | Flask/FastAPI 读 status JSON | 无后端 |
| **额外依赖** | Node.js + Redis | Flask 或 FastAPI | 无（`python -m http.server`） |
| **与 daemon-less 兼容** | 冲突（需 Node 进程常驻） | 需一个 Python 进程 | 完全兼容 |
| **实时性** | 高（WebSocket/轮询） | 中（HTTP 轮询 ~2s） | 低（文件刷新间隔） |
| **多机支持** | 天然支持 | 需额外汇聚逻辑 | 仅单机 |
| **开发量** | 高（引入整套 Node 栈） | 中 | 低 |

### 初步倾向

- **单机 (V2)**: 方案 B 或 C 足够，不破坏 daemon-less 架构
- **多机 (V3)**: WandB 自带 dashboard 可覆盖部分需求；如需自建则方案 B + 各机 agent 上报

## 3. 方案 B 草案（轻量 Python Web）

```
DAGRunner (已有)               Web Server (新增)
    │                              │
    ├─ 写 registry.json           ├─ GET /api/gpus
    ├─ 写 status/*.json           │   → 读 registry.json + pynvml
    │                              ├─ GET /api/pipeline
    │                              │   → 读 status/*.json + dag 定义
    │                              ├─ GET /
    │                              │   → 静态 HTML + JS (auto-refresh)
    │                              │
    └── (互不依赖，各自运行)       └── flask/fastapi 进程
```

**启动方式**:
```bash
# 与 pyturbo run 分开启动，不耦合
pyturbo monitor --port 8080 --registry ./registry.json --status-dir ./status/
```

**关键接口**:
```python
# GET /api/gpus -> JSON
{
    "gpus": [
        {"id": 0, "memory_used_mb": 4096, "memory_total_mb": 24576,
         "utilization_pct": 85, "task_id": "T1-train", "pid": 12345},
        {"id": 1, "memory_used_mb": 0, "memory_total_mb": 24576,
         "utilization_pct": 0, "task_id": null, "pid": null}
    ]
}

# GET /api/pipeline -> JSON
{
    "tasks": [
        {"id": "T1-train", "status": "running", "gpu_ids": [0,1,2,3],
         "depends_on": [], "progress_pct": 65},
        {"id": "T1-eval", "status": "pending", "gpu_ids": null,
         "depends_on": ["T1-train"], "progress_pct": 0}
    ],
    "edges": [["T1-train", "T1-eval"], ["T3-train", "T3-eval"]]
}
```

## 4. 方案 C 草案（静态 HTML 生成）

```python
# DAGRunner 主循环中每轮额外调用:
def generate_dashboard_html(registry, status_dir, output_path="dashboard.html"):
    """读 registry + status，生成一个自包含的 HTML 文件。"""
    ...

# 用户用浏览器打开 dashboard.html，页面内 meta refresh 自动刷新
# 或者: python -m http.server 8080 --directory ./output/
```

**优点**: 零依赖、零进程、DAGRunner 退出后最终状态仍可查看
**缺点**: 刷新间隔受 poll-interval 限制、无交互能力

## 5. video-ui-platform 可复用的前端组件

如果最终选方案 A 或 B 需要前端，以下 video-ui-platform 组件可参考：

| 组件 | 来源文件 | 复用价值 |
|---|---|---|
| WorkerManager | `frontend/src/components/WorkerManager.jsx` | GPU 状态卡片布局 |
| PipelineProgress | `frontend/src/components/PipelineProgress.jsx` | 进度条 + 步骤展示 |
| GPU 状态轮询逻辑 | `frontend/src/hooks/useWorkerStatus.js` | 定时 fetch + 状态合并 |

## 6. Coding Checklist（待启动）

| Task ID | 文件路径 | 操作 | 具体内容 |
|---|---|---|---|
| V3b-01 | `pyturbo/scheduler/dashboard.py` | **新增** | Flask/FastAPI 路由：`/api/gpus`, `/api/pipeline`, 静态文件 |
| V3b-02 | `pyturbo/scheduler/static/index.html` | **新增** | 前端页面：GPU 面板 + Pipeline DAG 可视化 |
| V3b-03 | `pyturbo/scheduler/cli.py` | **修改** | 新增 `pyturbo monitor` 子命令 |
| V3b-04 | `tests/test_dashboard.py` | **新增** | API 端点测试 |

## 7. 待决事项

- [ ] 确认选择方案 B 还是方案 C（或混合）
- [ ] 确认前端是否需要 DAG 拓扑图可视化（如需要，可能引入 D3.js 或 mermaid）
- [ ] 确认多机场景下是否需要自建 Web UI 还是直接用 WandB dashboard
- [ ] 确认 `pyturbo monitor` 是独立进程还是 `pyturbo run --with-dashboard` 内嵌

---

# QA

## 基础 QA

| 问题 | 结论 | 依据 |
|---|---|---|
| 1) 是否实现基于 GPU 动态数量的生产者/消费者模式? | 是 | Scheduler 产出 runnable, Allocator/Executor 消费并分配 gpu_required |
| 2) 是否实现基于 Cloud Code Task 文件格式管理? | 是 | TaskLoader 直接映射 blockedBy/blocks/status/description |
| 3) Claude Code Task 系统 vs PyTurbo(PTB) | 不同但可互补 | CC 偏任务描述/状态文件 + hooks, PTB 偏调度/资源执行引擎 |
| 4) 可借用点 | 任务文件格式与轻量状态 | blockedBy/blocks + status 持久化 |
| 5) video-ui-platform 与 PTB v2 的关系 | 设计模式可复用，代码不直接复用 | 见 QA-5 详细分析 |
| 6) video-ui-platform 中哪些模式被采纳? | 3 项（GPU 隔离 / PID 生命周期 / finally 清理） | 已写入 V2 §10 |
| 7) video-ui-platform 中哪些被弃用? | Redis 依赖 / Web UI / Daemon 模式 | PTB v2 走 daemon-less + 文件锁路线，不引入 Redis |

## QA-需求对齐（原 §1.2，从实现规格中移出）

| 需求 | 结论 | 具体落地 | 风险/注意 |
|---|---|---|---|
| block 语义 | B 串行 + A 互斥检测 | BlockManager 串行队列 + 互斥标记 | 同组任务不可并发 |
| GPU 空闲判定 | Registry + Monitor | 双重检查同意后分配 | 防止 registry 滞后 |
| Job 状态表 | 必须写 status | status/<task_id>.json | status 作为完成判定 |
| GPU 数量 | 非固定 | TaskSpec.gpu_required | 支持 1/2/3/4... |
| 失败策略 | 超限失败传播 | FailurePropagate | 分支前序+后续失败 |

## QA-核心设计原则（原 §1.3，从实现规格中移出）

| 原则 | 说明 | 示例 |
|---|---|---|
| 外层调度 | PTB 负责调度, runner 不改 | Executor 调用 runner |
| 任务级调度 | Job=实验任务 | T1-train / T1-eval |
| 资源独立 | GPU 统一分配 | Allocator 输出 gpu_ids |
| 状态持久化 | 运行状态可恢复 | status JSON 文件 |

## QA-架构模式转变讨论（原 §3.1，从实现规格中移出）

*   **现状判定**: 当前 `pyturbo` (v1) 是一个 **并行数据处理流水线引擎**，适用于单任务内的数据切分与并行处理，不具备多任务调度能力。
*   **v2 决策**: PyTurbo v2 将作为一个**全新的独立模块** (命名空间 `pyturbo.scheduler`) 构建，而不是在现有 `System/Pipeline` 类上修改。
*   **关系定义**:
    *   **Scheduler (v2)**: 负责全局资源视图、DAG 依赖管理、任务排队。
    *   **Executor (v1)**: 现有的 Pipeline 代码保留，可作为 Scheduler 调度的具体"任务载体"之一（即一个 Job 内部可能运行一个 PyTurbo Pipeline）。

## QA-5: video-ui-platform 代码库观察

**参考来源**: `tmp_pyturbo-v2/video-ui-platform/`（React + Node.js + Redis + Python Worker 的分布式视频处理平台）

<details>
<summary>video-ui-platform 架构概要</summary>

| 层 | 技术 | 说明 |
|---|---|---|
| 前端 | React 19 + Vite + TailwindCSS | WorkerManager / PipelineProgress 组件 |
| 后端 | Express.js + Multer + Redis | API 路由、任务队列、Worker 注册 |
| Worker | Python (`pipeline_redis.py`) | GPU 上的 ML 推理 pipeline |
| 状态 | Redis Hash + List | 任务队列 (`LPUSH/RPOP`) + Worker 心跳 + Step 进度 |
| GPU 隔离 | `CUDA_VISIBLE_DEVICES` env var | 每个 Worker 独占一块 GPU |

</details>

<details>
<summary>可复用模式 vs 不复用部分 对比表</summary>

**已采纳的模式（写入 V2 §10）:**

| 模式 | video-ui-platform 实现 | PTB v2 对应实现 | 采纳位置 |
|---|---|---|---|
| GPU 进程隔离 | `spawn(python, {env: {CUDA_VISIBLE_DEVICES: gpuId}})` | Executor 启动 subprocess 时注入 `CUDA_VISIBLE_DEVICES` | V2 §10.1 Executor |
| PID 生命周期追踪 | `workerProcesses.set(gpuId, {process, startTime, gpuId})` | Registry 中记录 `pid` 字段，用于 stale entry 检测 | V2 §12.2 Registry |
| 资源释放保证 | `finally { hashProcessingLocks.delete(hash); }` | Executor 在 `finally` 块中释放 Registry 占位 | V2 §10.1 Executor |
| 心跳判活（概念） | Redis heartbeat: `Date.now() - timestamp < 30000` | PID 存活检查: `os.kill(pid, 0)` | V2 §12.2 Stale Entry |

**未采纳的部分:**

| 部分 | 原因 |
|---|---|
| Redis 作为状态存储 | PTB v2 核心决策是 daemon-less + 文件锁，不引入外部依赖 |
| Node.js 后端 + Express | PTB v2 是纯 Python CLI 工具 |
| React 前端 + 轮询 UI | PTB v2 无 Web UI，状态通过 CLI 输出和 status JSON 文件呈现 |
| Multer 文件上传流 | 不相关 |
| FFmpeg 视频转码 | 不相关 |

</details>

## QA-8: 无 Redis 的 GPU 监控与生产者/消费者模式

<details>
<summary>问题背景与适用场景</summary>

PTB v2 选择了 daemon-less + 文件锁路线，不引入 Redis。核心问题：**不用 Redis 怎么实现 GPU 的持续监控和生产者/消费者协调？**

| 版本 | 场景 | 机器拓扑 |
|---|---|---|
| **v2（当前）** | 单台机器，多 GPU | 1 台 × N 卡 |
| v3（未来） | 多台机器，每台多 GPU | M 台 × N 卡 |

以下讨论均针对 **v2 单机多卡** 场景。

</details>

### 当前设计的监控模式分析

当前设计**没有真正的生产者/消费者**——每个 `pyturbo run` 进程都是"自给自足"的：

```
pyturbo run (进程 A)                pyturbo run (进程 B)
    │                                   │
    ├─ flock(registry.lock)             ├─ flock(registry.lock)  ← 被阻塞
    ├─ pynvml 查 GPU                    │   (等待 A 释放锁)
    ├─ 读 registry.json                 │
    ├─ 有空闲 GPU? → 写入占位           │
    ├─ 释放 flock                       ├─ 获得锁
    ├─ 执行任务...                      ├─ pynvml 查 GPU
    │                                   ├─ 读 registry → 发现 GPU 被占
    │                                   ├─ 释放 flock
    │                                   ├─ sleep 30s → 重试...
```

**这不是生产者/消费者，这是竞争抢占。** 隐患：

1.  **重复劳动**: N 个进程同时轮询，每个都调 pynvml
2.  **无通知机制**: GPU 释放后最多等 30s 才发现
3.  **公平性缺失**: 无 FIFO 保证

### 三种可行方案对比

| | 方案 A: 纯轮询 (当前设计) | 方案 B: inotify 通知优化 | 方案 C: Unix Socket Broker |
|---|---|---|---|
| **模式** | 每个进程独立 poll | 文件变更触发唤醒 | 单独 monitor 进程分发 |
| **GPU 检查** | 每个进程各自调 pynvml | 同 A，但减少无效轮询 | 只有 broker 调 pynvml |
| **通知延迟** | poll-interval (30s) | 接近实时 (~100ms) | 接近实时 |
| **公平性** | 无保证 | 无保证 | FIFO 队列保证 |
| **额外依赖** | 无 | `inotify` (Linux 内建) | 需要一个 broker 进程 |
| **复杂度** | 低 | 中 | 高（引入了 daemon） |
| **适用并发** | 2-5 进程 | 5-10 进程 | 10+ 进程 |

<details>
<summary>方案 A 详细说明: 纯轮询</summary>

```
while True:
    lock = flock(registry.lock)
    gpu_state = pynvml.query_all_gpus()
    registry = read(registry.json)
    available = compute_available(gpu_state, registry)
    if len(available) >= gpu_required:
        registry.jobs.append(my_job)
        write(registry.json)
        unlock(lock)
        execute_task(available[:gpu_required])
        break
    else:
        unlock(lock)
        sleep(poll_interval)  # 30s 固定间隔
```

**够用的条件**: 并发进程不超过 5 个，30s poll interval 可以接受。

</details>

<details>
<summary>方案 B 详细说明: inotify 监听 registry 文件变更</summary>

```python
import inotify.adapters

i = inotify.adapters.Inotify()
i.add_watch(registry_path)

while not resource_acquired:
    if try_acquire_gpu():
        break
    for event in i.event_gen(timeout_s=poll_interval):
        if event and 'IN_MODIFY' in event[1]:
            break  # registry 变了，立即重新尝试
```

**优点**: GPU 释放后 ~100ms 唤醒；保持 daemon-less；inotify 是 Linux 内建；fallback 到纯轮询。
**缺点**: thundering herd（多个等待者同时唤醒），但 flock 保证只有一个成功，v2 并发 ≤ 10 开销可忽略。

</details>

<details>
<summary>方案 C 详细说明: 轻量 Unix Socket Broker</summary>

```
                    ┌─────────────────────┐
                    │    ptb-monitor      │  ← 唯一的 GPU 监控者
                    │  (Unix Domain Socket)│
                    │                     │
                    │  - pynvml 定时轮询  │
                    │  - 维护 GPU 全局视图│
                    │  - FIFO 等待队列    │
                    └──┬──────────────┬───┘
                       │              │
              request_gpu(n=4)  request_gpu(n=4)
                       │              │
                ┌──────┴──┐    ┌─────┴──────┐
                │ runner A │    │  runner B  │
                └─────────┘    └────────────┘
```

**这其实就是 Redis 的极简版** — 用 Unix Domain Socket 替代 Redis。
**优点**: 真正的生产者/消费者，FIFO 公平，pynvml 只调一次。
**缺点**: 引入了 daemon，违背 daemon-less 决策。

</details>

### QA-8 决策结论

| 问题 | 结论 | 依据 |
|---|---|---|
| 8) 无 Redis 如何实现 GPU 监控? | **方案 A + B 混合**: 纯轮询 + inotify 优化 | 保持 daemon-less；inotify 是 Linux 内建；fallback 到纯轮询 |
| 8a) 为何不选方案 C? | v2 并发 ≤ 10，竞争抢占足够 | Broker 引入 daemon，与 daemon-less 决策矛盾 |
| 8b) 公平性问题? | v2 暂不解决 | thundering herd 代价可忽略；v3 再考虑 FIFO |
| 8c) v3 多机场景? | 方案 C 或等价跨机协调层 | 多机需跨机 IPC（Redis / gRPC） |

## QA-9: Manager 模式选型 — daemon-less CLI 能否覆盖需求？

### 关键区分: DAG Runner vs Daemon

```
Daemon:      启动 → 永久运行，等待外部提交任务 → 手动停止
DAG Runner:  启动 → 加载 dag.json → 编排所有任务 → 全部完成 → 自动退出
```

**需求是 DAG Runner，不是 Daemon。** DAG Runner 是普通 CLI 进程，只不过生命周期覆盖整个 DAG 执行。与 daemon-less 完全兼容。

### 需求覆盖分析

| 需求 | daemon-less CLI 能否做 | 怎么做 |
|---|---|---|
| DAG 编排（T1-train → T1-eval） | **能** | `pyturbo run dag.json` 加载整个 DAG，循环调度直到全部完成后退出 |
| 并行执行不同 GPU 组 | **能** | 同时 spawn 多个子进程，用 `select/poll` 等待任意一个完成 |
| 完成通知 + 状态更新 | **能** | `proc.wait()` 返回 → 写 status.json → 检查 DAG 下游 |
| v3 扩展到远端机器 | **需要抽象层** | 替换 `LocalExecutor` → `RemoteExecutor`（见 V3 §4） |

### v2 完整执行流示例

```
$ pyturbo run --blocking dag.json

[LOAD] 加载 4 个任务: T1-train, T1-eval, T3-train, T3-eval
[DAG]  T1-train → T1-eval,  T3-train → T3-eval
[SCAN] 可运行: T1-train, T3-train (无前置依赖)

[GPU]  flock → pynvml → GPU 0-3 空闲, GPU 4-7 空闲
[DISPATCH] T1-train → GPU 0-3 (pid=12345)
[DISPATCH] T3-train → GPU 4-7 (pid=12346)

... (等待子进程) ...

[DONE] T1-train 完成 (exit=0), 释放 GPU 0-3
[STATUS] 写入 status/T1-train.json (state=success)
[DAG]  T1-eval 解除阻塞，加入可运行队列

[GPU]  flock → GPU 0-3 空闲
[DISPATCH] T1-eval → GPU 0-3 (pid=12347)

... (T3-train 和 T1-eval 并行执行) ...

[DONE] T3-train 完成, T1-eval 完成
[DISPATCH] T3-eval → GPU 4-7

[DONE] 所有任务完成, DAG Runner 退出
```

### QA-9 决策结论

| 问题 | 结论 | 依据 |
|---|---|---|
| 9) daemon-less CLI 能否覆盖 `dispatch(machine_id, task_id)`? | **能。** DAG Runner 模式 | 长生命周期 CLI，非 daemon |
| 9a) 是否需要独立 Manager daemon? | **不需要** | DAG Runner 就是 Manager，有明确生命周期 |
| 9b) v2 需要为 v3 预埋什么? | `Executor` 和 `MessageBus` 抽象接口 | 见 V2 §12.9 |
| 9c) v2 = 单机多卡, v3 = 多机多卡 | v2 中 `machine_id` = `"localhost"` | v3 替换 Executor 实现 |
