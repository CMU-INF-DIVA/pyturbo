from queue import Queue
from typing import List

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

    def __init__(self,  stages: List[Stage]):
        super(SyncPipeline, self).__init__(stages)

    def start(self):
        for stage in self.stages:
            stage.init()

    def run_task(self, task, current_stage=0):
        if current_stage == len(self.stages):
            yield task
            return
        current_result = self.stages[current_stage].run(task)
        if isinstance(current_result, Task):
            yield from self.run_task(current_result, current_stage + 1)
        else:
            for r in current_result:
                yield from self.run_task(r, current_stage + 1)

    def run(self, tasks):
        for task in tasks:
            yield from self.run_task(task)

    def reset(self):
        for stage in self.stages:
            stage.reset()

    def end(self):
        return

    def join(self, timeout=1):
        return

    def terminate(self):
        return


class AsyncPipeline(Pipeline):

    '''
    Asynchronized pipeline powered by multiprocessing.
    '''

    def __init__(self,  stages: List[Stage], job_queue_size: int = 32):
        super(AsyncPipeline, self).__init__(stages)
        self.manager = mp.Manager()
        self.job_queue = mp.Queue(job_queue_size)
        self.worker_groups = []
        job_queue = self.job_queue
        for stage_i in range(len(stages)):
            stage = stages[stage_i]
            next_stage = stages[stage_i + 1] if stage_i + 1 < len(stages) \
                else None
            group = WorkerGroup(stage, next_stage, job_queue, self.manager)
            self.worker_groups.append(group)
            job_queue = group.result_queue
        self.result_queue = job_queue

    def start(self):
        for group in self.worker_groups:
            group.start()

    def reset(self):
        reset_task = ControlTask(ControlCommand.Reset)
        for _ in range(self.stages[0].num_worker):
            self.job_queue.put(reset_task)

    def wait(self):
        control_task_count = self.stages[-1].num_worker
        while control_task_count > 0:
            result = self.result_queue.get()
            if isinstance(result, Task):
                yield result
            else:
                control_task_count -= 1

    def end(self):
        end_task = ControlTask(ControlCommand.End)
        for _ in range(self.stages[0].num_worker):
            self.job_queue.put(end_task)
        for _ in self.wait():
            pass

    def join(self, timeout=1):
        for group in self.worker_groups:
            group.join(timeout)

    def terminate(self):
        for group in self.worker_groups:
            group.terminate()
