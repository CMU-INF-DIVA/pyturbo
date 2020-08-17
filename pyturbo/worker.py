from typing import Union

from retrying import retry

from .runtime import QUEUE_EXCEPTIONS, mp
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
        self.control_barrier = mp.Barrier(stage.num_worker)
        self.processes = []
        for worker_id in range(stage.num_worker):
            process = Worker(
                stage, worker_id, job_queue, self.result_queue,
                self.control_barrier,
                next_stage.num_worker if next_stage is not None else 1)
            self.processes.append(process)

    def start(self):
        for process in self.processes:
            process.start()

    def join(self, timeout=1):
        for process in self.processes:
            process.join(timeout)
        for process in self.processes:
            if process.exitcode is not None:
                process.close()

    def terminate(self, timeout=1):
        for process in self.processes:
            process.terminate()
        self.join(timeout)


class Worker(mp.Process):

    '''
    One worker process.
    '''

    def __init__(self, stage: Stage, worker_id: int,
                 job_queue: mp.Queue, result_queue: mp.Queue,
                 control_barrier: mp.Barrier, next_num_worker: int):
        name = '%s-%d' % (stage.__class__.__name__, worker_id)
        super(Worker, self).__init__(name=name)
        self.stage = stage
        self.worker_id = worker_id
        self.job_queue = job_queue
        self.result_queue = result_queue
        self.control_barrier = control_barrier
        self.next_num_worker = next_num_worker

    def control(self, task):
        if task.command == ControlCommand.Reset:
            self.stage.reset()
        pass_id = self.control_barrier.wait()
        if pass_id == 0:
            for _ in range(self.next_num_worker):
                self.result_queue.put(task)
        if task.command == ControlCommand.End:
            return True

    # In case Pytorch exhausts system's shared memory
    @retry(retry_on_exception=lambda e: isinstance(e, RuntimeError),
           wait_random_min=100, wait_random_max=2000)
    def put_queue(self, content):
        self.result_queue.put(content)

    def run(self):
        self.stage.init(self.worker_id)
        while True:
            try:
                try:
                    task = self.job_queue.get()
                except QUEUE_EXCEPTIONS:
                    break
                if isinstance(task, ControlTask):
                    if self.control(task):
                        return
                    continue
                result = self.stage.run(task)
                if result is None:
                    continue
                if isinstance(result, Task):
                    result = [result]
                for r in result:
                    try:
                        self.put_queue(r)
                    except QUEUE_EXCEPTIONS:
                        break
            except (KeyboardInterrupt, GeneratorExit):
                break
