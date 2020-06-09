import time
from collections import namedtuple
from enum import IntEnum, auto
from typing import Any, Union

ControlCommand = IntEnum('ControlCommand', ['End', 'Reset'])


class ControlTask(object):
    '''
    Pipeline control task.
    '''

    def __init__(self, command: ControlCommand):
        self.command = command

    def __repr__(self):
        return super(ControlTask, self).__repr__(self.command.name)


TaskLog = namedtuple('TaskLog', [
    'stage', 'duration', 'start_time', 'end_time'])


class Task(object):
    '''
    Regular excutable task.
    '''

    def __init__(self, content: Any, meta: Union[None, dict] = None, *,
                 parent_task=None):
        self.create_time = time.time()
        self.content = content
        self._build_meta(meta, parent_task)
        self.logs = parent_task.logs.copy() if parent_task is not None else []

    def _build_meta(self, meta, parent_task):
        self.meta = {}
        if parent_task is not None:
            self.meta.update(parent_task.meta)
        if meta is not None:
            self.meta.update(meta)

    def start(self, stage: str):
        self.start_time = time.time()
        self.current_stage = stage

    def finish(self):
        self.finish_time = time.time()
        duration = self.finish_time - self.start_time
        log = TaskLog(
            self.current_stage, duration, self.start_time, self.finish_time)
        self.logs.append(log)

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, repr(self.meta))
