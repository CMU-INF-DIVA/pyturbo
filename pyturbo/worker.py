from typing import Union

from .runtime import mp
from .stage import Stage
from .task import ControlCommmand, ControlTask, RegularTask


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
            process = Worker(self, worker_id)
            process.start()
            self.processes.append(process)


class Worker(mp.Process):

    '''
    One worker process.
    '''

    def __init__(self, group: WorkerGroup, worker_id: int):
        name = '%s-%d' % (group.stage.name, worker_id)
        super(Worker, self).__init__(name=name)
        self.control_sequence = 0
        self.worker_id = worker_id
        self.group = group

    def control(self, task):
        if task.command == ControlCommmand.End:
            return
        elif task.command == ControlCommmand.Reset:
            self.stage.reset()
        pass_id = self.group.control_barrier.wait()
        if pass_id == 0:
            for _ in range(self.group.next_stage.worker_num):
                self.group.result_queue.put(task)

    def run(self):
        self.group.stage.init(self.worker_id)
        while True:
            try:
                task = self.group.job_queue.get()
            except (EOFError, FileNotFoundError, BrokenPipeError):
                break
            if isinstance(task, ControlTask):
                self.control(task.command)
                continue
            result = self.group.stage.process(task)
            if isinstance(result, RegularTask):
                self.group.result_queue.put(result)
            else:
                for r in result:
                    self.group.result_queue.put(r)
