```text
                                               ┌───────────────────────────┐
CN VPS / local dev                             │        WandB platform     │
+────────────────────────────────────────────┐ │(Experiment + Automations) │
│                                            │ │                           │
│  +-------------------+   REST API / CLI    │ │ +-----------------------+ │
│  |  Claude Code UI   | <------------------>│ │ |  Experiments Metrics  | │
│  | (code / analysis) |                     │ │ |(Logs/Charts/Artifacts)| │
│  +-------------------+                     │ │ +-----------------------+ │
│              ^                             │ │    ^            ^         │
│              |                             │ │    |            |         │
│              |   API Pull (WandB API)      │ │   Webhook Trigger         │
│              |                             │ │    |            |         │
│  +------------------------+              ┌-──v──-┐             │         │
│  |  Trigger / Webhook SRV | <----------->│WandB  │---------> GitHub      │
│  | (handle events/exec)   |              │Auto-  │ Trigger Repo │Actions │
│  +------------------------+              │mations│             │         │
│              ^                           └──────-┘             │         │
│              |                                                 v         │
└──────────────┴────────────────────────────────────────> GPU Runner Nodes │
                                                          (Self-Hosted)    │
                                                          +----------------+
                                                          | GPU Exec Jobs  |
                                                          | Train/Infer    |
                                                          +----------------+
```

# 基于 WandB + Webhook + GitHub Actions 的分布式科研实验自动化方案

## 1. 背景与问题定义

在当前科研工作流中，存在如下现实约束：

- **Vibe coding / 思考速度受模型延迟影响显著**
  - 在美国使用 GLM / Kimi / MiniMax 等模型，延迟偏高
  - 在中国或亚洲区域，这些模型延迟明显更低，更适合“高频交互式编码”

- **GPU 资源集中在美国或固定机房**
  - 训练、推理、实验执行依赖远端 GPU 机器
  - 不希望频繁手动 SSH 登录或人工调度实验

- **Cloud Code（CC）目前具备“本地写代码 + 直接跑 GPU 实验”的优势**
  - 但当编码机器与 GPU 机器物理分离（如中国 VPS + 美国 GPU）时，该能力天然丢失

因此核心问题是：

> **能否将“信息链路”和“计算链路”解耦，在不牺牲实验自动化与可观测性的前提下，把编码位置迁移到中国，从而显著降低模型延迟？**

---

## 2. 总体设计目标

本方案的目标不是“重新发明一套调度系统”，而是：

- **最大化复用成熟工具**
- **最小化自研基础设施**
- **让研究者专注在科研本身，而非运维**

具体目标如下：

1. 编码与思考发生在 **中国 VPS**
2. GPU 训练 / 推理发生在 **远端 GPU 机器**
3. 实验日志、指标、状态 **统一由 WandB 管理**
4. 任务触发通过 **Webhook / CI 自动化完成**
5. Claude Code / 人类均可通过 WandB **观察、分析、对比实验**

---

## 3. 核心架构概览
```text
┌──────────────────────────────┐
│   CN VPS (low-latency dev)   │
│                              │
│  Claude Code / Editor        │
│  - Coding                    │
│  - Plan experiments          │
│  - View results via API      │
│                              │
│  No direct GPU access        │
└───────────────┬──────────────┘
│
│ (Git / Webhook / API)
▼
┌──────────────────────────────┐
│      Central SoT system      │
│          WandB               │
│                              │
│  - Experiment logs           │
│  - Metrics / curves          │
│  - Artifact / model version  │
│  - Automations / Webhook     │
└───────────────┬──────────────┘
│
│ (Webhook / CI Trigger)
▼
┌──────────────────────────────┐
│     GPU machines (compute)   │
│                              │
│  GitHub Actions              │
│  Self-hosted GPU Runner      │
│                              │
│  - Pull code                 │
│  - Run train / inference     │
│  - Write results to WandB    │
└──────────────────────────────┘
```

---

## 4. 职责拆分（非常关键）

### 4.1 WandB 的不可替代角色（核心）

WandB 在本方案中承担的是**“事实中枢（single source of truth）”**：

- 实验是否成功
- 实验的指标、loss、曲线
- 使用了哪一版代码 / 哪一版模型
- 哪个实验触发了下一个实验

**结论：WandB 的角色在此方案中不可替代。**

这是目前唯一一个同时满足：

- 实验级可观测性
- API 可访问
- 自动化事件触发
- 大规模科研实践验证

的现成平台。

---

### 4.2 GitHub Actions / Runner 的角色（可替代，但成熟）

GitHub Actions 在这里的作用是：

- **代码同步的“事实执行者”**
- **远端 GPU 任务的启动器**

是否可以替代？
- 可以被 **GitLab CI / 自建 CI / Slurm wrapper** 等替代
- 但 GitHub Actions 的优势在于：
  - 成熟
  - 大量现成模式
  - 支持 self-hosted runner（GPU）

**结论：不是唯一方案，但当前性价比最高、工程复杂度最低。**

---

### 4.3 Webhook 的角色（信息链路，而非数据链路）

Webhook 只负责一件事：

> **“某个事件发生了”**

例如：
- 某个实验结束
- 某个指标达到阈值
- 某个 artifact 被注册

Webhook **不承担数据传输**，也不承担日志同步。

**结论：Webhook 能力完全满足需求，且是 WandB 官方一等支持能力。**

---

## 5. 两条“数据链路”的澄清（关键认知点）

你的判断是**完全正确的**：这里存在两条彼此独立的数据路径。

### 5.1 代码链路（Code Path）

- 方式：
  - Git push
  - GitHub Actions checkout
- 作用：
  - 保证 GPU 机器上执行的是“哪一版代码”

这是**静态、版本化**的数据。

---

### 5.2 信息 / 实验链路（Info Path）

- 方式：
  - WandB SDK 上报
  - WandB API 查询
  - WandB Webhook 触发
- 作用：
  - 实验状态
  - 指标
  - 日志
  - 分析结果

这是**动态、时序化**的数据。

**结论：这两条链路天然解耦，是一个正确且可扩展的设计。**

---

## 6. Claude Code 在该架构中的真实能力边界

必须明确一件事：

- Claude Code **不再直接 SSH / 直接跑 GPU**
- Claude Code 的角色转变为：
  - 代码生成者
  - 实验设计者
  - 实验结果分析者

Claude Code 通过：

- WandB API 读取实验结果
- 人类或自动系统触发下一轮实验

来完成“科研闭环”。

**结论：功能是“等价的”，只是从“直接控制”变为“间接控制”。**

---

## 7. 自动化程度评估

### 能做到的（确认可行）

- ✅ 自动触发远端 GPU 实验
- ✅ 自动记录所有实验日志与指标
- ✅ 自动对比多次实验结果
- ✅ 在中国低延迟使用 GLM / Kimi / MiniMax 进行编码
- ✅ 不需要人工登录 GPU 机器
- ✅ 不需要手动 rsync 日志

### 做不到 / 不建议做的

- ❌ Claude Code 直接 attach GPU
- ❌ 用 Webhook 传输大规模日志（反模式）
- ❌ 自研一整套 WandB 等价系统（成本极高）

---

## 8. 与 Cloud Code 原生体验的对比

| 维度 | CC 单机 | 本方案 |
|----|----|----|
| 编码延迟 | 受限于模型区域 | **可显著降低（中国）** |
| GPU 控制 | 直接 | 间接 |
| 实验记录 | 弱 / 分散 | **强 / 统一（WandB）** |
| 可扩展性 | 低 | **高** |
| 工程复杂度 | 低 | 中（一次性） |
| 长期科研效率 | 中 | **高** |

---

## 9. 最终可行性结论（重要）

> **结论：该方案是“现实可行、工程成熟、已被大量 MLOps / 科研团队验证”的方案。**

- 所依赖的关键能力：
  - WandB Automations / Webhook ✅ 官方支持
  - WandB API 查询实验结果 ✅ 官方支持
  - GitHub Actions + GPU Runner ✅ 广泛实践
- 没有任何“需要 hack / 未公开接口 / 灰色能力”的部分
- 复杂度主要集中在**一次性的架构搭建**
- 一旦搭好，**长期科研效率显著提升**

---

## 10. 总结一句话版本（给 report 用）

> 通过将编码与计算解耦，并以 WandB 作为实验事实中枢，我们可以在中国低延迟环境中进行高频科研编码，同时在远端 GPU 集群上自动化执行实验，最终在不牺牲实验可观测性与可复现性的前提下，显著提升整体科研效率。

# 11. 实操落地补充 (2026-01-31)

针对 "GitHub Actions 并发触发导致 GPU 资源竞争" 的问题，我们在 PyTurbo v2 实施中确定了 **"Gatekeeper (守门员)"** 模式：

*   **Daemon-less Design**: GPU Runner 节点上**不需要**运行额外的 System Daemon。
*   **CLI Gatekeeper**: GitHub Action Runner 调用 `pyturbo run` 命令。
*   **File Lock Coordination**: 该命令利用 Linux 文件锁 (`flock`) 自动处理并发排队。
    *   如果 GPU 满了，Action 会自动阻塞等待 (Spin wait)。
    *   一旦有空闲 GPU，Action 抢占锁并开始执行。
    *   这样可以完美利用 GitHub Actions 自身的 Job Timeout 和 Logging 机制，无需维护复杂的自定义调度服务。