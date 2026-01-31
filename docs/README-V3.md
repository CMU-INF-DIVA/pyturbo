# PyTurbo Scheduler V3 — 多机远程执行使用说明

> V3 在 V2 的基础上扩展了多机远程执行能力。DAGRunner 核心循环不变，通过替换 Executor 和 MessageBus 实现远程调度。

## 前置条件

- V2 已安装（`pip install -e ".[scheduler]"`）
- 额外依赖视执行方式而定：

| 执行方式 | 依赖 |
|---|---|
| GitHub Actions (`gh`) | [GitHub CLI](https://cli.github.com/) 已安装并认证 (`gh auth login`) |
| SSH | SSH 密钥已配置，目标机器可免密登录 |
| WandB | `pip install wandb` 并已登录 (`wandb login`) |

## 快速开始

### 1. 编写远程配置文件

`remote_config.json`：

```json
{
  "machines": [
    {
      "machine_id": "gpu-server-1",
      "hostname": "10.0.0.1",
      "gpu_count": 8,
      "runner_label": "gpu",
      "ssh_user": "wenhel",
      "ssh_key": "~/.ssh/id_rsa",
      "work_dir": "/home/wenhel/workspace"
    },
    {
      "machine_id": "gpu-server-2",
      "hostname": "10.0.0.2",
      "gpu_count": 4,
      "runner_label": "gpu-small",
      "ssh_user": "wenhel"
    }
  ],
  "wandb": {
    "project": "dsta-stance",
    "entity": "my-team",
    "poll_interval": 30
  },
  "github_repo": "owner/repo",
  "default_workflow": "run-experiment.yml"
}
```

<details>
<summary>配置字段说明</summary>

**machines[]:**

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `machine_id` | str | 是 | 机器唯一标识 |
| `hostname` | str | 是 | IP 或域名 |
| `gpu_count` | int | 是 | GPU 数量 |
| `runner_label` | str | 否 | GitHub Actions self-hosted runner 标签 |
| `ssh_user` | str | 否 | SSH 用户名（SSH 模式必填） |
| `ssh_key` | str | 否 | SSH 私钥路径 |
| `work_dir` | str | 否 | 远程工作目录（默认 `~`） |

**wandb:**

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `project` | str | `dsta-stance` | WandB 项目名 |
| `entity` | str | null | WandB 团队/用户 |
| `poll_interval` | int | 30 | 状态轮询间隔（秒） |

**顶层:**

| 字段 | 类型 | 说明 |
|---|---|---|
| `github_repo` | str | GitHub 仓库，如 `owner/repo` |
| `default_workflow` | str | 默认 workflow 文件名 |

</details>

### 2. 编写任务定义

与 V2 格式完全一致，参见 [README-V2.md](README-V2.md)。

### 3. 运行

```bash
# 方式一：通过 GitHub Actions 远程执行
pyturbo run --blocking --remote --remote-config remote_config.json --remote-method gh dag.json

# 方式二：通过 SSH 直接远程执行
pyturbo run --blocking --remote --remote-config remote_config.json --remote-method ssh dag.json

# 试运行
pyturbo run --dry-run --remote --remote-config remote_config.json dag.json
```

## V3 新增 CLI 参数

以下参数在 V2 基础上新增，仅在 `--remote` 模式下生效：

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `--remote` | flag | False | 启用远程执行模式 |
| `--remote-config` | str | — | 远程配置 JSON 文件路径（`--remote` 时必填） |
| `--machine` | str | — | 指定目标机器 ID |
| `--remote-method` | str | `gh` | 远程执行方式：`gh`（GitHub Actions）或 `ssh` |

## 两种执行方式对比

| | GitHub Actions (`gh`) | SSH |
|---|---|---|
| **触发方式** | `gh workflow run` 触发 workflow_dispatch | `ssh user@host` 直接执行命令 |
| **状态轮询** | `gh run view --json status,conclusion` | SSH 子进程 `poll()` |
| **前置要求** | repo 中有 `.github/workflows/run-experiment.yml` + self-hosted runner | SSH 免密登录 |
| **适用场景** | 正式实验、需要 CI 记录 | 快速调试、临时执行 |
| **日志** | GitHub Actions 控制台 | 本地 stdout |

## GitHub Actions 模式详解

### 工作流模板

V3 包含一个 workflow 模板 `.github/workflows/run-experiment.yml`：

```yaml
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

### 执行流程

```
本地 VPS                         远程 GPU 机器 (self-hosted runner)
────────                         ───────────────────────────────────
pyturbo run --remote
    │
    ├─ gh workflow run ──────→  GitHub Actions 收到 dispatch
    │                               │
    ├─ gh run view (轮询) ←────  runner 执行 pyturbo run --blocking
    │                               │
    │                               ├─ 本地 GPU 分配 (V2 逻辑)
    │                               ├─ 执行训练/评估
    │                               └─ wandb.log() 上报指标
    │
    ├─ 检测到 run completed
    ├─ 更新 DAG 状态
    └─ 调度下一个任务
```

## SSH 模式详解

### 执行流程

```
本地 VPS                         远程 GPU 机器
────────                         ───────────
pyturbo run --remote --remote-method ssh
    │
    ├─ ssh user@host ──────────→  CUDA_VISIBLE_DEVICES=0,1,2,3 <command>
    │       │
    │       ├─ 子进程 poll()
    │       │
    │   exit code ←────────────  任务完成
    │
    ├─ 更新 DAG 状态
    └─ 调度下一个任务
```

SSH 模式下 `CUDA_VISIBLE_DEVICES` 直接注入到远程命令中。

## WandB 集成

V3 使用 `WandBMessageBus` 在多机间传递事件：

```python
# Runner 端（GPU 机器）上报指标
import wandb
run = wandb.init(project="dsta-stance", name="T1-train", tags=["T1", "train"])
wandb.log({"accuracy": 0.76, "loss": 0.35, "epoch": 3})
wandb.finish()

# VPS 端查询
import wandb
api = wandb.Api()
runs = api.runs("dsta-stance", filters={"tags": {"$in": ["T1"]}})
for r in runs:
    print(r.name, r.state, r.summary.get("accuracy"))
```

`WandBMessageBus` 封装了上述逻辑，DAGRunner 通过 `bus.publish()` / `bus.subscribe()` 透明调用。

## 架构：V2 → V3 的变化

V3 通过接口替换实现扩展，DAGRunner 核心循环零改动：

```
V2 (单机)                          V3 (多机)
─────────                          ─────────
LocalExecutor                  →   RemoteExecutor (gh / ssh)
  subprocess.Popen                   gh workflow dispatch / ssh
FileMessageBus                 →   WandBMessageBus
  status.json + inotify             wandb.log() + API query
(无)                           →   RemoteConfig
                                     machines + wandb + github_repo
```

## 测试

```bash
# 运行所有测试（含 V3 mock 测试，无需真实远程环境）
python -m pytest tests/ -v

# 只运行 V3 测试
python -m pytest tests/test_remote_executor.py tests/test_wandb_bus.py -v
```

## 目录结构（V3 新增文件）

```
pyturbo/scheduler/
├── ...                          # V2 文件（见 README-V2.md）
├── config.py                    # [V3] RemoteConfig, MachineConfig, WandBConfig
├── remote_executor.py           # [V3] RemoteExecutor (gh + ssh)
└── wandb_bus.py                 # [V3] WandBMessageBus

.github/workflows/
└── run-experiment.yml           # [V3] workflow_dispatch 模板
```
