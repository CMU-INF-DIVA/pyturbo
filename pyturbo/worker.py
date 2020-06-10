from typing import Union

from .runtime import mp
from .stage import Stage
from .task import ControlCommand, ControlTask, Task


class WorkerGroup(object):

    '''
    A group of processes for one stage.
    '''

    def __init__(self, stage: Stage, next_stage: Union[None, Stage],
                 job_queue: mp.Queue,  manager: mp.Manager):
        self.stage = stage
        self.next_stage = next_stage
        self.job_queue = job_queue
        self.manager = manager
        self.result_queue = manager.Queue()
        self.control_barrier = mp.Barrier(stage.worker_num)
        self.processes = []
        for worker_id in range(stage.worker_num):
            process = Worker(
                stage, worker_id, job_queue, self.result_queue,
                self.control_barrier,
                next_stage.worker_num if next_stage is not None else 1)
            self.processes.append(process)

    def start(self):
        for process in self.processes:
            process.start()

    def join(self, timeout=1):
        for process in self.processes:
            process.join(timeout)

    def terminate(self):
        for process in self.processes:
            process.terminate()


class Worker(mp.Process):

    '''
    One worker process.
    '''

    def __init__(self, stage: Stage, worker_id: int,
                 job_queue: mp.Queue, result_queue: mp.Queue,
                 control_barrier: mp.Barrier, next_worker_num: int):
        name = '%s-%d' % (stage.__class__.__name__, worker_id)
        super(Worker, self).__init__(name=name)
        self.stage = stage
        self.worker_id = worker_id
        self.job_queue = job_queue
        self.result_queue = result_queue
        self.control_barrier = control_barrier
        self.next_worker_num = next_worker_num

    def control(self, task):
        if task.command == ControlCommand.Reset:
            self.stage.reset()
        pass_id = self.control_barrier.wait()
        if pass_id == 0:
            for _ in range(self.next_worker_num):
                self.result_queue.put(task)
        if task.command == ControlCommand.End:
            return True

    def run(self):
        self.stage.init(self.worker_id)
        while True:
            try:
                task = self.job_queue.get()
            except (EOFError, FileNotFoundError, BrokenPipeError):
                break
            if isinstance(task, ControlTask):
                if self.control(task):
                    return
                continue
            result = self.stage.process(task)
            if isinstance(result, Task):
                self.result_queue.put(result)
            else:
                for r in result:
                    self.result_queue.put(r)
