from collections import deque
from typing import Iterable, Union

from .resource import Resources
from .task import Task
from .utils import get_logger


class Stage(object):

    '''
    One stage in a pipeline. A stage can have multiple peer worker processes.
    Worker number is desided based on available resources.
    If no resource is provided for allocation, defaults to max_worker or 1.
    '''

    def __init__(self, resources: Union[None, Resources] = None,
                 max_worker: Union[None, int] = None,
                 result_queue_size: int = 32):
        self.resources = resources
        if resources is not None:
            self.resource_allocation = resources.allocate(
                self.get_resource_unit())
            self.worker_num = len(self.resource_allocation)
            if max_worker is not None:
                self.worker_num = min(self.worker_num, max_worker)
                self.resource_allocation = self.resource_allocation[
                    :self.worker_num]
        else:
            if max_worker is None:
                max_worker = 1
            self.resource_allocation = [{} for _ in range(max_worker)]
            self.worker_num = max_worker
        self.result_queue_size = result_queue_size
        self.logger = None

    def get_resource_unit(self):
        '''
        Resource requirement for each worker.
        '''
        return {'cpu': 0.1}

    def reset(self):
        '''
        Reset a worker process. Automatically executed during initialization.
        '''
        self.logger.debug('Reset.')

    def process(self, task: Task) -> Union[Task, Iterable[Task]]:
        '''
        Process function for each worker process. 
        Returns one or a series of downstream tasks.
        '''
        raise NotImplementedError

    def init(self, worker_id: int = 0):
        self.worker_id = worker_id
        self.current_resource = self.resource_allocation[worker_id]
        name = '%s[%d]%s' % (self.__class__.__name__, worker_id,
                             '@' + str(self.current_resource)
                             if self.current_resource is not None else '')
        self.logger = get_logger(name)
        self.logger.debug('Init.')
        self.reset()

    def run(self, task):
        return self.process(task)

    def __repr__(self):
        return '%s(x%d)%s' % (
            self.__class__.__name__, self.worker_num,
            '@' + str(self.resources) if self.resources is not None else '')


class ReorderStage(Stage):

    '''
    A special stage with only one worker. 
    Tasks are processed according to a pre-defined order.
    '''

    def __init__(self, resources: Union[None, Resources] = None,
                 result_queue_size: int = 32):
        super(ReorderStage, self).__init__(resources, 1, result_queue_size)

    def get_sequence_id(self, task: Task) -> int:
        '''
        Return the order of each task. Start from 0.
        '''
        raise NotImplementedError

    def reset(self):
        super(ReorderStage, self).reset()
        self.reorder_buffer = deque()
        self.next_id = 0

    def push(self, task: Task):
        offset = self.get_sequence_id(task) - self.next_id
        if offset >= len(self.reorder_buffer):
            self.reorder_buffer.extend([None] * (
                offset - len(self.reorder_buffer) + 1))
        self.reorder_buffer[offset] = task

    def iter_pop(self):
        while len(self.reorder_buffer) > 0:
            if self.reorder_buffer[0] is not None:
                task = self.reorder_buffer.popleft()
                yield task

    def run(self, task):
        self.push(task)
        for task in self.iter_pop():
            res = self.process(task)
            if isinstance(res, Task):
                yield res
            else:
                for r in res:
                    yield r
