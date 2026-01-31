# PyTurbo 代码库结构与函数摘要

## 目录结构(简表)

| 路径 | 类型 | 说明 |
|---|---|---|
| /home/wenhel/dsta-stance-project/pyturbo | 根目录 | PyTurbo 源码与发布文件根目录 |
| /home/wenhel/dsta-stance-project/pyturbo/pyturbo | 包目录 | 核心库实现 |
| /home/wenhel/dsta-stance-project/pyturbo/docs | 文档目录 | 项目内部文档与规划 |
| /home/wenhel/dsta-stance-project/pyturbo/.github | CI 配置 | GitHub Actions 工作流 |
| /home/wenhel/dsta-stance-project/pyturbo/.git | 版本库 | Git 元数据 |

## 脚本与模块一览(一行描述)

| 文件 | 一句话描述 |
|---|---|
| /home/wenhel/dsta-stance-project/pyturbo/pyturbo/__init__.py | 对外导出核心类与工具函数, 作为包入口。 |
| /home/wenhel/dsta-stance-project/pyturbo/pyturbo/resource.py | 资源扫描与分配, 提供 CPU/GPU 资源管理工具。 |
| /home/wenhel/dsta-stance-project/pyturbo/pyturbo/system.py | 系统级调度与任务执行入口, 管理 pipeline 与 job 生命周期。 |
| /home/wenhel/dsta-stance-project/pyturbo/pyturbo/stage.py | 定义 Stage 与 ReorderStage, 提供任务处理与排序逻辑。 |
| /home/wenhel/dsta-stance-project/pyturbo/pyturbo/pipeline.py | 定义同步/异步 pipeline, 组织 stage 顺序执行。 |
| /home/wenhel/dsta-stance-project/pyturbo/pyturbo/task.py | 定义 Task/ControlTask 与任务日志结构。 |
| /home/wenhel/dsta-stance-project/pyturbo/pyturbo/runtime.py | 运行时配置与 multiprocessing 上下文设置。 |
| /home/wenhel/dsta-stance-project/pyturbo/pyturbo/worker.py | worker 进程与 worker 组管理。 |
| /home/wenhel/dsta-stance-project/pyturbo/pyturbo/utils.py | 日志与进度条工具函数。 |
| /home/wenhel/dsta-stance-project/pyturbo/demo.py | 示例 pipeline 与 toy system 演示。 |
| /home/wenhel/dsta-stance-project/pyturbo/setup.py | 打包与发布配置脚本。 |

## 函数/方法摘要(按文件)

### /home/wenhel/dsta-stance-project/pyturbo/pyturbo/resource.py

| 函数/方法 | 一句话描述 |
|---|---|
| Resources.__init__ | 以给定资源或扫描结果初始化资源集合。 |
| Resources.__add__ | 合并两个 Resources 的资源列表。 |
| Resources.get | 按名称获取资源列表并支持 limit。 |
| Resources.scan | 扫描 CPU/GPU 资源并返回初始资源字典。 |
| Resources.split | 按 pipeline 数量均分资源。 |
| Resources.select | 按选项裁剪资源子集(支持比例与切片)。 |
| Resources._index_or_frac | 将分数范围转换为索引范围。 |
| Resources.__str__ | 返回资源摘要字符串。 |
| Resources.__repr__ | 返回调试用表示字符串。 |

### /home/wenhel/dsta-stance-project/pyturbo/pyturbo/system.py

| 函数/方法 | 一句话描述 |
|---|---|
| Job.__init__ | 构建 Job, 绑定初始 Task 与元数据。 |
| Job.finish | 标记 Job 成功并记录 pipeline_id。 |
| Job.__repr__ | 生成 Job 的可读字符串。 |
| Result.__init__ | 初始化结果容器。 |
| Result.__len__ | 返回已收集任务数。 |
| Result.collect | 收集单个 Task 结果或失败。 |
| Result.merge | 合并/后处理结果(预留扩展)。 |
| System.__init__ | 初始化系统、资源、pipeline 与队列。 |
| System.get_num_pipeline | 返回 pipeline 数量(默认 1)。 |
| System.get_stages | 返回 pipeline 的 stage 列表(需实现)。 |
| System.collect_results | 将 pipeline 输出任务收集到 Result。 |
| System.monit_pipeline | 监控 pipeline 执行并处理 job 生命周期。 |
| System.build | 根据资源拆分构建 pipeline 实例。 |
| System.start | 启动 pipeline 与监控线程。 |
| System.add_job | 入队单个 job。 |
| System.add_jobs | 入队多个 job。 |
| System.wait_job | 等待单个 job 完成并处理重试。 |
| System.wait_jobs | 按数量等待多个 job 完成。 |
| System.end | 正常停止 pipeline 与清理线程。 |
| System.terminate | 异常终止 pipeline 并清理。 |
| System.__repr__ | 返回系统运行模式表示。 |

### /home/wenhel/dsta-stance-project/pyturbo/pyturbo/stage.py

| 函数/方法 | 一句话描述 |
|---|---|
| Stage.__init__ | 初始化 stage 并分配资源给 worker。 |
| Stage.allocate_resource | 默认分配资源给 worker(可覆写)。 |
| Stage.reset | 重置 worker 状态。 |
| Stage.process | 核心处理函数(需实现)。 |
| Stage.init | 初始化 worker 的资源与 logger。 |
| Stage.run | 执行任务并捕获异常。 |
| Stage.__repr__ | 返回 stage 资源与 worker 数量信息。 |
| ReorderStage.__init__ | 初始化排序 stage, 强制单 worker。 |
| ReorderStage.get_sequence_id | 返回任务排序序号(需实现)。 |
| ReorderStage.reset | 初始化排序缓冲区与序号。 |
| ReorderStage.push | 将任务加入排序缓冲区。 |
| ReorderStage.iter_pop | 依序弹出可处理任务。 |
| ReorderStage.run | 按序处理任务并输出结果。 |

### /home/wenhel/dsta-stance-project/pyturbo/pyturbo/pipeline.py

| 函数/方法 | 一句话描述 |
|---|---|
| Pipeline.__init__ | 保存 pipeline 的 stage 列表。 |
| SyncPipeline.start | 初始化所有 stage 并 reset。 |
| SyncPipeline.run_task | 递归执行单个任务穿过所有 stage。 |
| SyncPipeline.run | 执行任务列表并逐个处理。 |
| SyncPipeline.reset | 重置所有 stage。 |
| SyncPipeline.end | 同步 pipeline 结束(空实现)。 |
| SyncPipeline.join | 同步 pipeline join(空实现)。 |
| SyncPipeline.terminate | 同步 pipeline 终止(空实现)。 |
| AsyncPipeline.__init__ | 构建多进程 worker group 与队列。 |
| AsyncPipeline.start | 启动 worker 并执行 reset。 |
| AsyncPipeline.reset | 通过控制任务重置 pipeline。 |
| AsyncPipeline.wait | 等待任务结果或 reset 完成。 |
| AsyncPipeline.end | 发送结束控制任务并回收。 |
| AsyncPipeline.join | 等待 worker 进程退出。 |
| AsyncPipeline.terminate | 强制终止所有 worker。 |

### /home/wenhel/dsta-stance-project/pyturbo/pyturbo/task.py

| 函数/方法 | 一句话描述 |
|---|---|
| ControlTask.__init__ | 构建控制任务(End/Reset)。 |
| ControlTask.__repr__ | 返回控制任务字符串。 |
| Task.__init__ | 构建任务, 初始化 meta 与日志。 |
| Task._build_meta | 继承/合并任务元数据。 |
| Task.start | 记录任务开始时间与 stage。 |
| Task.finish | 记录结束时间与结果, 写入日志。 |
| Task.fail | 标记任务失败并记录日志。 |
| Task.__repr__ | 返回任务元数据表示。 |

### /home/wenhel/dsta-stance-project/pyturbo/pyturbo/runtime.py

| 函数/方法 | 一句话描述 |
|---|---|
| Options.__init__ | 从环境变量解析运行时选项。 |
| Options.__setattr__ | 设置属性并处理 log_file。 |
| Options.__str__ | 返回可读配置字符串。 |
| Options.__repr__ | 返回调试表示字符串。 |

### /home/wenhel/dsta-stance-project/pyturbo/pyturbo/worker.py

| 函数/方法 | 一句话描述 |
|---|---|
| WorkerGroup.__init__ | 创建一个 stage 对应的 worker 组。 |
| WorkerGroup.start | 启动所有 worker 进程。 |
| WorkerGroup.join | 等待 worker 退出。 |
| WorkerGroup.terminate | 强制终止 worker 并回收。 |
| Worker.__init__ | 初始化单个 worker 进程。 |
| Worker.control | 处理控制任务(reset/end)。 |
| Worker.run | 循环获取任务并执行 stage.run。 |

### /home/wenhel/dsta-stance-project/pyturbo/pyturbo/utils.py

| 函数/方法 | 一句话描述 |
|---|---|
| progressbar | 基于 tqdm 的可选进度条包装。 |
| get_logger | 构造带控制台/文件输出的 logger。 |
| process_map | 多进程 map 并带进度条输出。 |

### /home/wenhel/dsta-stance-project/pyturbo/pyturbo/__init__.py

| 项目 | 一句话描述 |
|---|---|
| 模块导出 | 导出 Options, Stage, System, Task, utils 等。 |

### /home/wenhel/dsta-stance-project/pyturbo/demo.py

| 函数/方法 | 一句话描述 |
|---|---|
| Stage1.process | 将 (x,y) 展开为一系列子任务。 |
| Stage2.allocate_resource | 按 CPU 数量分配 worker 资源。 |
| Stage2.process | 计算 x * 7。 |
| Stage3.allocate_resource | 加大 worker 数量并按 CPU 切片。 |
| Stage3.process | 计算 int(x/7)。 |
| Stage4.get_sequence_id | 按元数据 i 保序输出。 |
| Stage4.process | 计算 -x。 |
| ToySystem.get_num_pipeline | 依据 CPU 数量设置 pipeline 数量。 |
| ToySystem.get_stages | 构建示例 stages 并做资源分配。 |
| main | 运行 toy system 并验证结果。 |

### /home/wenhel/dsta-stance-project/pyturbo/setup.py

| 项目 | 一句话描述 |
|---|---|
| setup | 使用 setuptools 定义包发布元数据。 |
