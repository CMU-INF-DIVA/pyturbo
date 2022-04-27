import os
import warnings
from copy import deepcopy
from typing import Any, Dict, List, Optional

import GPUtil
import psutil


class Resources(object):

    '''
    Resource management and allocation.
    '''

    def __init__(self, resources: Dict[str, List[Any]] = None, **scan_args):
        if resources is not None:
            self.resources = resources
        else:
            self.resources = self.scan(**scan_args)

    def __add__(self, other):
        new_resources = deepcopy(self.resources)
        for key, value in other.resources.items():
            new_resources.setdefault(key, []).extend(value)
        return self.__class__(new_resources)

    def get(self, name: str, limit: Optional[int] = None):
        return self.resources.get(name, [])[:limit]

    def scan(self, gpu_load: float = 0.1, gpu_memory: float = 0.1):
        '''
        Initial global scan for a system.
        '''
        resources = {'cpu': psutil.Process().cpu_affinity()}
        try:
            gpus = GPUtil.getAvailable(
                limit=100, maxLoad=gpu_load, maxMemory=gpu_memory)
            visible_gpus = os.environ.get('CUDA_VISIBLE_DEVICES')
            if visible_gpus is not None:
                gpus = [*map(int, visible_gpus.split(','))]
                # Remove env for correct GPU indices in subprocesses
                del os.environ['CUDA_VISIBLE_DEVICES']
            if len(gpus) > 0:
                resources['gpu'] = gpus
        except Exception as e:
            warnings.warn(
                f'GPU resources unavailable due to scan failure: {repr(e)}', RuntimeWarning)
        return resources

    def split(self, num_split: int):
        '''
        Even split between pipelines in a system, drop remainders.
        '''
        results = [{} for _ in range(num_split)]
        for key, value in self.resources.items():
            split_size = len(value) // num_split
            for split_i in range(num_split):
                results[split_i][key] = value[
                    split_i * split_size: (split_i + 1) * split_size]
        results = [Resources(r) for r in results]
        return results

    def select(self, **select_dict):
        '''
        Unbalanced select for stages in a pipeline.
        select_dict value: True for all, False for None, 
            (start, end) in int/float/None for slice
        '''
        selected_resources = {}
        for key, value in select_dict.items():
            if not key in self.resources:
                continue
            resource = self.resources[key]
            if value is True:
                selected_resources[key] = resource
            elif value is False:
                continue
            else:
                start, end = self._index_or_frac(value, len(resource))
                selected_resources[key] = resource[start:end]
        return Resources(selected_resources)

    @staticmethod
    def _index_or_frac(values, length):
        is_float = any([isinstance(value, float) for value in values])
        if not is_float:
            return values
        return [int(round(value * length)) if value is not None else None
                for value in values]

    def __str__(self):
        return ', '.join(['%s=%s' % (k, v) for k, v in self.resources.items()])

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, str(self))
