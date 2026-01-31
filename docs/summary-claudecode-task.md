# Claude Code Task 设计与实现方式调研报告

## 结论摘要

在 `claude-code-is-fun/claude-code` 内, **任务(task)的“实现”主要体现在文档与插件约定中**, 而非完整的运行时代码。任务的核心设计点是: **用 `.claude/*.local.md` 文件(含 YAML frontmatter + markdown body)存储任务元信息与依赖**, 再由 hooks/commands 解析并驱动行为。任务依赖与状态更偏“配置与流程控制”, 而非内建的调度引擎。

## 主要证据来源

- 版本日志(任务管理/依赖/背景任务等功能演进):
  - `/home/wenhel/dsta-stance-project/claude-code-is-fun/claude-code/CHANGELOG.md`
- 任务/状态文件格式与解析方法(插件设置模式):
  - `/home/wenhel/dsta-stance-project/claude-code-is-fun/claude-code/plugins/plugin-dev/skills/plugin-settings/SKILL.md`
  - `/home/wenhel/dsta-stance-project/claude-code-is-fun/claude-code/plugins/plugin-dev/skills/plugin-settings/examples/example-settings.md`
- 真实案例(依赖字段、状态更新、hook 读取等):
  - `/home/wenhel/dsta-stance-project/claude-code-is-fun/claude-code/plugins/plugin-dev/skills/plugin-settings/references/real-world-examples.md`
- 任务列表文件(交互式任务创建流程):
  - `/home/wenhel/dsta-stance-project/claude-code-is-fun/claude-code/plugins/plugin-dev/skills/command-development/references/interactive-commands.md`

## Task 的“数据结构”与存储方式

### 1) 核心载体: `.claude/*.local.md`

- 位置: 项目根目录 `.claude/`
- 结构: YAML frontmatter + markdown body
- 作用: 存储任务元数据、依赖、状态与说明

示例(来自真实案例与模板):

```
---
agent_name: auth-implementation
task_number: 3.5
pr_number: 1234
coordinator_session: team-leader
enabled: true
dependencies: ["Task 3.4"]
additional_instructions: "Use JWT tokens, not sessions"
---

# Task: Implement Authentication

Build JWT-based authentication for the REST API.
```

说明:
- `task_number` 与 `dependencies` 表示依赖关系
- markdown body 保存任务描述/成功条件
- hooks/commands 读取该文件决定行为

### 2) 任务列表文件

在交互式命令指南中, 任务列表文件会被写入:

- `.daisy/swarm/tasks.md`

用于集中存放多任务定义, 但实现细节仍以文档约定为主。

## Task 依赖/状态如何运作

### 依赖字段

- `dependencies: ["Task 3.4"]` 是常见写法
- 依赖的解释与执行是由 hooks/commands 消费, 而不是引擎级调度

### 状态更新

示例中用脚本更新 frontmatter 字段(如 PR number 等):

```
sed "s/^pr_number: .*/pr_number: $PR_NUM/" \
  ".claude/multi-agent-swarm.local.md" > temp.md
mv temp.md ".claude/multi-agent-swarm.local.md"
```

这说明任务状态更多是文件级状态更新, 而非内建任务数据库。

## Task 执行与通知机制

### Hook 驱动

- 典型做法是在 hook 中读取 `.claude/*.local.md`
- 解析 frontmatter 后触发动作(如 tmux 通知)

示例(简化):

- 读取 `coordinator_session` / `agent_name` / `task_number`
- 发送通知给协调会话

这体现了“任务 -> hooks -> 操作”的设计路线。

## Task 工具与后台任务(产品层特性)

从 CHANGELOG 中可看到 Claude Code 的 task 能力主要体现在产品功能:

- 任务管理系统与依赖跟踪(2.1.16)
- `/tasks` 对话框(后台任务可视化)
- 任务通知与结果展示
- Task tool 可用于启动子代理

这些点说明 Claude Code 在产品层面具备任务调度/后台化能力, 但在本仓库中主要呈现为文档/规则描述。

## 和我们需求的映射启示

1) **任务元数据的存储方式**
   - Claude Code 使用 frontmatter + markdown 的结构化文件
   - 我们可借鉴该模式, 用 status/task 文件存储依赖与状态

2) **依赖机制是“数据驱动”**
   - 依赖字段只是一种描述, 真正执行依赖逻辑的是调度器/脚本
   - 我们的 PTB v2 需要显式实现依赖调度

3) **状态与日志是文件级持久化**
   - Claude Code 的任务状态通常写在本地文件中, 易于恢复
   - 我们可以采用类似“job status 文件”策略

## 局限与待补齐

- 当前 `claude-code-is-fun/claude-code` 目录以文档/插件模板为主
- 未找到完整的 task 调度引擎代码实现(仅见 hooks/commands 级别的脚本示例)
- 若需要更底层的实现细节, 可能需要访问 Claude Code 的核心源码仓库

## 2026-01-31 14:54

### 基于本地 tasks JSON 的补充观察

从 `/home/wenhel/dsta-stance-project/dsta-stance-clean-refactor-s2/tmp_pyturbo-v2/tasks` 读取到的任务记录显示:

- 任务文件以目录分组(看起来是 project/session 维度), 每个任务为独立 JSON 文件
- 关键字段固定为: `id`, `subject`, `description`, `activeForm`, `status`, `blocks`, `blockedBy`
- `description` 里包含 Markdown 命令块, GPU 组信息、命令行、预期结果等都放在非结构化文本中
- `blockedBy` 表示依赖前置任务 ID, `blocks` 表示阻塞/后继任务 ID (显式 DAG 边)
- `status` 以字符串表示状态(如 `pending`, `completed`)
- 目录中存在 `.lock` 文件(为空), 可能用于并发写入/互斥控制

这些 JSON 更像是“任务元数据+依赖”的轻量持久化, 可以直接映射到我们 PTB v2 的 TaskSpec/StatusStore 设计:

- `blockedBy` -> deps
- `blocks` -> downstream/children
- `status` -> Job 状态机
- `description` -> 可作为任务说明/命令来源, 但需结构化字段以支持 GPU/资源调度
