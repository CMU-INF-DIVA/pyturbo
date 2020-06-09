from typing import Union

from .task import Task
from .utils import get_logger


class Stage(object):

    '''
    One stage in a pipeline. A stage can have multiple peer worker processes.
    '''

    def __init__(self, name: str, resources: Union[None, list] = None,
                 resource_per_worker: Union[int, float] = 1,
                 result_queue_size: int = 32):
        self.name = name
        self.resources_by_worker = self._allocate(
            resources, resource_per_worker)
        self.current_resource = None
        self.worker_num = len(self.resources_by_worker)
        self.result_queue_size = result_queue_size
        self.logger = None

    def _allocate(self, resources, split_size):
        if isinstance(split_size, int):
            resources_by_worker = [
                resources[start:start + split_size]
                for start in range(0, len(resources), split_size)]
        else:
            resources_by_worker = resources * int(1 / split_size)
        return resources_by_worker

    def __repr__(self):
        return '%s(%s)@%s' % (
            self.__class__.__name__, self.name, self.current_resource)

    def _init(self, worker_id: int = 0):
        self.logger = get_logger(repr(self))
        self.current_resource = self.resources_by_worker[worker_id]
        self.reset()

    def reset(self):
        '''
        Reset a worker process. Automatically executed during initialization.
        '''
        pass

    def process(self, task: Task):
        '''
        Process function for each worker process
        '''
        raise NotImplementedError
