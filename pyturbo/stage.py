from collections import deque
from typing import Iterable, List, Union

from .resource import Resources
from .runtime import Options
from .task import Task
from .utils import get_logger


class Stage(object):

    '''
    One stage in a pipeline. A stage can have multiple peer worker processes.
    Worker number is desided based on how available resources are allocated.
    '''

    def __init__(self, resources: Resources, **custom_args):
        self.resources = resources
        self.logger = get_logger(repr(self))
        self.resource_allocation = self.allocate_resource(
            resources, **custom_args)
        self.num_worker = len(self.resource_allocation)

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
        name = '%s[%d]@(%s)' % (self.__class__.__name__, worker_id,
                                self.current_resource)
        self.logger = get_logger(name)

    def run(self, task: Task) -> Union[Task, Iterable[Task]]:
        assert isinstance(task, Task), '%s is not a subclass of %s' % (
            type(task), Task)
        try:
            if not task.success:
                self.logger.warn('Bypassing: %s', task)
                yield task
                return
            self.logger.debug('Processing: %s', task)
            result = self.process(task)
            if isinstance(result, Task):
                yield result
            else:
                for res in result:
                    yield res
            self.logger.debug('Processed: %s', task)
        except Exception as e:
            task.fail()
            self.logger.exception('Failed: %s', task)
            if Options.raise_exception:
                raise e
            else:
                yield task

    def __repr__(self):
        if hasattr(self, 'num_worker'):
            return '%s(x%d)@(%s)' % (self.__class__.__name__, self.num_worker,
                                     self.resources)
        return self.__class__.__name__


class ReorderStage(Stage):

    '''
    A special stage with only one worker. 
    Tasks are processed according to a pre-defined order.
    '''

    def __init__(self, resources: Resources, reorder_buffer_size: int = 16,
                 **custom_args):
        super(ReorderStage, self).__init__(resources, **custom_args)
        assert self.num_worker == 1, 'ReorderStage must have only 1 worker.'
        self.reorder_buffer_size = reorder_buffer_size
        self.reorder_buffer_size_record = reorder_buffer_size

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
        try:
            offset = self.get_sequence_id(task) - self.next_id
            if offset >= len(self.reorder_buffer):
                self.reorder_buffer.extend([None] * (
                    offset - len(self.reorder_buffer) + 1))
            self.reorder_buffer[offset] = task
        except:
            if len(self.reorder_buffer) > 0 and self.reorder_buffer[0] is None:
                self.reorder_buffer[0] = task
            else:
                self.reorder_buffer.insert(0, task)
        if len(self.reorder_buffer) > self.reorder_buffer_size_record:
            self.reorder_buffer_size_record = len(self.reorder_buffer)
            self.logger.warn(
                'Reorder buffer size %d exceeds limit of %d',
                len(self.reorder_buffer), self.reorder_buffer_size)

    def iter_pop(self) -> Iterable[Task]:
        while len(self.reorder_buffer) > 0:
            if self.reorder_buffer[0] is not None:
                task = self.reorder_buffer.popleft()
                yield task
                try:
                    self.next_id = self.get_sequence_id(task) + 1
                except:
                    pass
            else:
                break

    def run(self, task) -> Iterable[Task]:
        assert isinstance(task, Task)
        self.logger.debug('Enqueue: %s', task)
        self.push(task)
        for task in self.iter_pop():
            try:
                if not task.success:
                    self.logger.warn('Bypassing: %s', task)
                    yield task
                    continue
                self.logger.debug('Processing: %s', task)
                result = self.process(task)
                if result is None:
                    continue
                elif isinstance(result, Task):
                    yield result
                else:
                    for res in result:
                        yield res
                self.logger.debug('Processed: %s', task)
            except Exception as e:
                task.fail()
                self.logger.exception('Failed: %s', task)
                if Options.raise_exception:
                    raise e
                else:
                    yield task
