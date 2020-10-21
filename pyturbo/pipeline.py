from itertools import zip_longest
from typing import Iterable, List, Optional

from .runtime import mp
from .stage import Stage
from .task import ControlCommand, ControlTask, Task
from .worker import WorkerGroup


class Pipeline(object):

    '''
    A pipeline consists of multiple stages that executes sequentially.
    '''

    def __init__(self, stages: List[Stage]):
        self.stages = stages


class SyncPipeline(Pipeline):

    '''
    A simple synchronized pipeline for debug. 
    All stages run in the main process.
    '''

    def __init__(self,  stages: List[Stage], queue_size: int = 16):
        super(SyncPipeline, self).__init__(stages)

    def start(self, timeout: Optional[int] = 1):
        for stage in self.stages:
            stage.init()
        self.reset()

    def run_task(self, task: Task, current_stage: int = 0) -> Iterable[Task]:
        if current_stage == len(self.stages):
            yield task
            return
        current_result = self.stages[current_stage].run(task)
        if isinstance(current_result, Task):
            yield from self.run_task(current_result, current_stage + 1)
        else:
            for r in current_result:
                yield from self.run_task(r, current_stage + 1)

    def run(self, tasks: List[Task]) -> Iterable[Task]:
        for task in tasks:
            yield from self.run_task(task)

    def reset(self, timeout: Optional[int] = 1):
        for stage in self.stages:
            stage.reset()

    def end(self, timeout: Optional[int] = 1):
        return

    def join(self, timeout: Optional[int] = 1):
        return

    def terminate(self, timeout: Optional[int] = 1):
        return


class AsyncPipeline(Pipeline):

    '''
    Asynchronized pipeline powered by multiprocessing.
    '''

    def __init__(self, stages: List[Stage], queue_size: Optional[int] = 16):
        super(AsyncPipeline, self).__init__(stages)
        self.manager = mp.Manager()
        self.job_queue = self.manager.Queue()
        self.worker_groups = []
        job_queue = self.job_queue
        for stage, next_stage in zip_longest(stages, stages[1:]):
            group = WorkerGroup(
                stage, job_queue, self.manager, queue_size, next_stage)
            self.worker_groups.append(group)
            job_queue = group.result_queue
        self.result_queue = job_queue
        self.resetting = False
        self.terminated = False

    def start(self, timeout: Optional[int] = 1):
        for group in self.worker_groups:
            group.start()
        self.reset(timeout)

    def reset(self, timeout: Optional[int] = 1):
        assert not self.resetting, 'Cannot reset when reset is in progress'
        reset_task = ControlTask(ControlCommand.Reset)
        for _ in range(self.stages[0].num_worker):
            self.job_queue.put(reset_task, timeout=timeout)
        self.resetting = True

    def wait(self, timeout: Optional[int] = None):
        while True:
            result = self.result_queue.get(timeout=timeout)
            if isinstance(result, ControlTask):
                if result.command == ControlCommand.Reset:
                    self.resetting = False
                break
            else:
                yield result

    def end(self, timeout: Optional[int] = 1):
        if self.terminated:
            return
        end_task = ControlTask(ControlCommand.End)
        for _ in range(self.stages[0].num_worker):
            self.job_queue.put(end_task, timeout=timeout)
        for _ in self.wait(timeout):
            pass

    def join(self, timeout: Optional[int] = 1):
        for group in self.worker_groups:
            group.join(timeout)

    def terminate(self, timeout: Optional[int] = 3):
        for group in self.worker_groups:
            group.terminate(timeout)
        self.terminated = True
