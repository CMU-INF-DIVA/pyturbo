from collections import deque
from typing import Iterable, List, Union

from .resource import Resources
from .runtime import Options
from .task import Task
from .utils import get_logger


class Stage(object):

    '''
    One stage in a pipeline. A stage can have multiple peer worker processes.
    Worker number is desided based on available resources.
    If no resource is provided for allocation, defaults to max_worker or 1.
    '''

    def __init__(self, resources: Union[None, Resources] = None,
                 result_queue_size: int = 32, **custom_args):
        self.resources = resources
        self.logger = get_logger(repr(self))
        self.resource_allocation = self.allocate_resource(
            resources, **custom_args)
        self.num_worker = len(self.resource_allocation)
        self.result_queue_size = result_queue_size

    def allocate_resource(self, resources: Resources,
                          **custom_args) -> List[Resources]:
        '''
        Allocate resources to each worker, the length of return value indicates 
        number of workers. Called during object initialization with the custom 
        keyword arguments.
        '''
        if len(custom_args) > 0:
            self.logger.warn('Unprocessed custom_args: %s', custom_args)
        return [resources]

    def reset(self):
        '''
        Reset a worker process. 
        Executed during initialization and at reset command.
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
        assert isinstance(task, Task), '%s is not a subclass of %s' % (
            type(task), Task)
        try:
            self.logger.debug('Processing: %s', task)
            result = self.process(task)
            if isinstance(result, Task):
                yield result
            else:
                for res in result:
                    yield res
            self.logger.debug('Processed: %s', task)
        except GeneratorExit:
            self.logger.warn('Stopped before complete: %s', task)
        except Exception as e:
            self.logger.exception('Failed: %s', task)
            if Options.raise_exception:
                raise e

    def __repr__(self):
        return '%s%s%s' % (
            self.__class__.__name__,
            '(x%d)' % (self.num_worker) if hasattr(self, 'num_worker') else '',
            '@' + str(self.resources) if self.resources is not None else '')


class ReorderStage(Stage):

    '''
    A special stage with only one worker. 
    Tasks are processed according to a pre-defined order.
    '''

    def __init__(self, resources: Union[None, Resources] = None,
                 result_queue_size: int = 32,
                 reorder_buffer_size: int = 32, **custom_args):
        super(ReorderStage, self).__init__(
            resources, result_queue_size, **custom_args)
        assert self.num_worker == 1, 'ReorderStage must have only 1 worker.'
        self.reorder_buffer_size = reorder_buffer_size

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
        if len(self.reorder_buffer) > self.reorder_buffer_size:
            self.logger.warn(
                'Reorder buffer size %d exceeds limit of %d',
                len(self.reorder_buffer), self.reorder_buffer_size)

    def iter_pop(self):
        while len(self.reorder_buffer) > 0:
            if self.reorder_buffer[0] is not None:
                task = self.reorder_buffer.popleft()
                yield task
                self.next_id = self.get_sequence_id(task) + 1
            else:
                break

    def run(self, task):
        assert isinstance(task, Task)
        self.logger.debug('Enqueue: %s', task)
        self.push(task)
        for task in self.iter_pop():
            try:
                self.logger.debug('Processing: %s', task)
                result = self.process(task)
                if isinstance(result, Task):
                    yield result
                else:
                    for res in result:
                        yield res
                self.logger.debug('Processed: %s', task)
            except GeneratorExit:
                self.logger.warn('Stopped before complete: %s', task)
            except Exception as e:
                self.logger.exception('Failed: %s', task)
                if Options.raise_exception:
                    raise e
