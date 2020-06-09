from typing import Iterable, Union

from .resource import Resource
from .task import Task
from .utils import get_logger


class Stage(object):

    '''
    One stage in a pipeline. A stage can have multiple peer worker processes.
    Worker number is desided based on available resources.
    If no resource is provided for allocation, defaults to 1 worker.
    '''

    def __init__(self, name: str,
                 resources: Union[None, Iterable[Resource]] = None,
                 max_worker: Union[None, int] = None,
                 result_queue_size: int = 32):
        self.name = name
        if resources is not None:
            self.resources = [dict(r) for r in zip(*resources)]
            self.worker_num = len(self.resources)
        else:
            self.resources = [None]
            self.worker_num = 1
        if max_worker is not None:
            self.worker_num = min(self.worker_num, max_worker)
            self.resources = self.resources[:self.worker_num]
        self.result_queue_size = result_queue_size
        self.logger = None

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
        self.current_resource = self.resources[worker_id]
        name = '%s(%s)[%d]@%s' % (
            self.__class__.__name__, self.name, self.worker_id,
            self.current_resource)
        self.logger = get_logger(name)
        self.logger.debug('Init.')
        self.reset()

    def __repr__(self):
        return '%s(%s)[%d]@%s' % (
            self.__class__.__name__, self.name, self.worker_num,
            self.resources)
