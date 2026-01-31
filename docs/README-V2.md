# PyTurbo Scheduler V2 — 使用说明

> 单机多卡 DAG 任务调度器。通过 `pyturbo run` 命令自动编排 GPU 任务的依赖、排队和执行。

## 安装

```bash
cd /home/wenhel/dsta-stance-project/pyturbo
pip install -e ".[scheduler]"
```

依赖项：`pynvml`（GPU 监控）、`inotify`（Linux 文件变更通知，可选）。

## 快速开始

### 1. 编写任务定义文件

单个任务 `task.json`：

```json
{
  "id": "T1-train",
  "subject": "Train T1 TikTok Qwen-Clf",
  "command": "bash /path/to/scripts/run_test_train_model.sh t1 {gpu_ids}",
  "gpu_required": 4,
  "blockedBy": [],
  "blocks": ["T1-eval"],
  "max_retry": 1,
  "priority": 1
}
```

多任务 DAG `dag.json`：

```json
[
  {
    "id": "T1-train",
    "subject": "Train T1",
    "command": "bash scripts/run_test_train_model.sh t1 {gpu_ids}",
    "gpu_required": 4,
    "blockedBy": [],
    "blocks": ["T1-eval"],
    "block_group": "gpu-group-1",
    "max_retry": 1,
    "priority": 1
  },
  {
    "id": "T1-eval",
    "subject": "Eval T1",
    "command": "bash scripts/run_test_evaltarget_model-deliver-ppl.sh t1 {gpu_ids}",
    "gpu_required": 4,
    "blockedBy": ["T1-train"],
    "blocks": [],
    "block_group": "gpu-group-1",
    "max_retry": 1,
    "priority": 2
  },
  {
    "id": "T3-train",
    "subject": "Train T3",
    "command": "bash scripts/run_test_train_model.sh t3 {gpu_ids}",
    "gpu_required": 4,
    "blockedBy": [],
    "blocks": ["T3-eval"],
    "block_group": "gpu-group-2",
    "max_retry": 1,
    "priority": 1
  },
  {
    "id": "T3-eval",
    "subject": "Eval T3",
    "command": "bash scripts/run_test_evaltarget_model-deliver-ppl.sh t3 {gpu_ids}",
    "gpu_required": 4,
    "blockedBy": ["T3-train"],
    "blocks": [],
    "block_group": "gpu-group-2",
    "max_retry": 1,
    "priority": 2
  }
]
```

**`{gpu_ids}`** 是占位符，执行时由调度器替换为实际分配的 GPU（如 `0,1,2,3`）。

### 2. 运行

```bash
# 基本用法：执行 DAG，资源不足时自旋等待
pyturbo run --blocking dag.json

# 试运行：只打印分配计划，不实际执行
pyturbo run --dry-run dag.json

# 覆盖所有任务的 GPU 数量
pyturbo run --blocking -g 2 dag.json

# 详细日志
pyturbo run --blocking -v dag.json
```

### 3. 观察输出

```
2026-01-31 15:08:00 INFO  [DAG] Loaded 4 tasks: ['T1-train', 'T1-eval', 'T3-train', 'T3-eval']
2026-01-31 15:08:00 INFO  [DAG] Summary: {'pending': 4}
2026-01-31 15:08:01 INFO  [DISPATCH] task=T1-train -> GPU [0, 1, 2, 3] (pid=12345)
2026-01-31 15:08:01 INFO  [DISPATCH] task=T3-train -> GPU [4, 5, 6, 7] (pid=12346)
...
2026-01-31 16:30:00 INFO  [DONE] task=T1-train completed successfully
2026-01-31 16:30:01 INFO  [DISPATCH] task=T1-eval -> GPU [0, 1, 2, 3] (pid=12347)
...
2026-01-31 17:00:00 INFO  [DAG] Completed. Summary: {'success': 4}
```

## CLI 参数完整列表

```
pyturbo run [OPTIONS] TASK_DEF
```

| 参数 | 短名 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `task_def` | — | 位置参数 | 必填 | 任务定义 JSON 文件路径 |
| `--gpu-required` | `-g` | int | 按任务定义 | 覆盖所有任务的 GPU 需求量 |
| `--blocking` | `-b` | flag | False | 资源不足时自旋等待（而非立即退出） |
| `--poll-interval` | `-p` | int (秒) | 30 | 自旋等待轮询间隔 |
| `--max-wait` | `-w` | int (秒) | 3600 | 最大等待时间，超时退出码 75 |
| `--registry-path` | `-r` | str | `~/.pyturbo/registry.json` | Registry 文件路径 |
| `--dry-run` | — | flag | False | 只打印分配计划，不执行 |
| `--gpu-ids` | — | str | 自动分配 | 手动指定 GPU，如 `0,1,2,3` |
| `--status-dir` | — | str | `status/` | 状态 JSON 文件目录 |
| `--config` | `-c` | str | — | 实验配置文件（透传给 runner） |
| `--mode` | `-m` | str | — | 执行模式 training/evaluation（透传） |
| `--model-path` | — | str | — | 模型路径（透传） |
| `--verbose` | `-v` | flag | False | 详细日志 |

## 退出码

| 退出码 | 含义 |
|---|---|
| `0` | 所有任务成功完成 |
| `1` | 有任务执行失败 |
| `2` | 参数错误 / 配置文件不存在 |
| `75` | 资源等待超时 |
| `77` | GPU 监控或 Registry 初始化失败 |

## 任务定义字段

<details>
<summary>TaskSpec 完整字段说明</summary>

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `id` | str | 是 | — | 任务唯一标识 |
| `subject` | str | 是 | — | 任务简述 |
| `command` | str | 是 | — | 执行命令，`{gpu_ids}` 会被替换 |
| `gpu_required` | int | 否 | 4 | 所需 GPU 数量 |
| `blockedBy` | list[str] | 否 | [] | 依赖的前置任务 ID 列表 |
| `blocks` | list[str] | 否 | [] | 本任务完成后解锁的下游任务 |
| `block_group` | str | 否 | null | 串行组 ID，同组任务顺序执行 |
| `max_retry` | int | 否 | 1 | 最大重试次数 |
| `priority` | int | 否 | 1 | 优先级，数字越小越优先 |
| `tags` | list[str] | 否 | [] | 标签（用于过滤） |
| `description` | str | 否 | "" | 任务详细描述 |
| `activeForm` | str | 否 | "" | 进行中状态的展示文本 |
| `status` | str | 否 | "pending" | 初始状态 |

</details>

## 核心概念

### DAG 依赖

通过 `blockedBy` / `blocks` 字段定义任务间的依赖关系。调度器自动进行拓扑排序，只有前置任务全部成功后才会执行后续任务。

```
T1-train ──→ T1-eval
T3-train ──→ T3-eval     (T1 和 T3 两条链路可并行)
```

### 串行组 (block_group)

同一 `block_group` 内的任务按 `priority` 排序，同一时间只允许一个运行。不同 block_group 之间互不影响。

### GPU 分配

调度器通过 **双重检查** 确定 GPU 可用性：

1. **Registry 检查**：`~/.pyturbo/registry.json` 中没有占用记录
2. **物理检查**：`pynvml` 检测显存 < 200MB 且利用率 < 5% 且无计算进程

两项同时满足才判定为空闲。如果 Registry 空闲但物理被占（ghost 进程），该 GPU 不可分配。

### 失败与重试

- 任务失败且 `retry_count < max_retry` → 自动重新排队
- 超过重试上限 → 标记为 `failed`，所有下游任务标记为 `branch_fail`

### 状态文件

每个任务的执行状态写入 `{status_dir}/{task_id}.json`：

```json
{
  "job_id": "T1-train",
  "state": "success",
  "start_time": "2026-01-31T15:08:00",
  "end_time": "2026-01-31T16:30:00",
  "gpu_ids": [0, 1, 2, 3],
  "exit_code": 0,
  "retry_count": 0
}
```

### Registry

GPU 占用注册表位于 `~/.pyturbo/registry.json`（可通过 `--registry-path` 自定义）。进程崩溃时 `flock` 自动释放，stale entry 在下次轮询时通过 PID 存活检查自动清理。

## 典型用法

```bash
# 场景 1: 跑完一整条 train → eval 链
pyturbo run --blocking dag.json

# 场景 2: 多个实验并发提交（各自占不同 GPU）
pyturbo run --blocking t1_tasks.json &
pyturbo run --blocking t3_tasks.json &
wait

# 场景 3: 只看分配计划
pyturbo run --dry-run dag.json

# 场景 4: 指定 registry 路径（多项目隔离）
pyturbo run --blocking -r /tmp/project-a/registry.json dag.json

# 场景 5: 通过 Python 模块调用
python -m pyturbo.scheduler.cli run --blocking dag.json
```

## 状态机

```
pending ──→ running ──→ success
              │
              ▼
           failed ←── retry (if retry_count < max_retry)
              │
              ▼
          branch_fail (downstream tasks)
```

## 测试

```bash
# 运行所有单元测试（无需 GPU）
python -m pytest tests/ -v

# 只运行 DAG runner 测试
python -m pytest tests/test_dag_runner.py -v

# 只运行 registry 测试
python -m pytest tests/test_registry.py -v
```

## 目录结构

```
pyturbo/scheduler/
├── __init__.py          # 模块导出
├── cli.py               # pyturbo run 入口
├── dataclasses.py       # TaskSpec, GPUState, JobStatus 等数据类
├── task.py              # TaskLoader + TaskGraph (DAG)
├── block.py             # BlockManager (串行组)
├── monitor.py           # NvidiaMonitor + RegistryWatcher (inotify)
├── registry.py          # FileLockRegistry (flock + JSON)
├── allocator.py         # ResourceAllocator (双重检查)
├── runner.py            # LocalExecutor + DAGRunner 核心循环
└── status.py            # StatusStore + FileMessageBus + FailurePropagator
```
