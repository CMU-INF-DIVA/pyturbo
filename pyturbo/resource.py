import os
from itertools import cycle
from typing import Any, Dict, List, Union

import GPUtil


class Resources(object):

    '''
    Resource management and allocation.
    '''

    def __init__(self, resources: Dict[str, List[Any]] = None, **scan_args):
        if resources is not None:
            self.resources = resources
        else:
            self.resources = self.scan(**scan_args)

    def scan(self, gpu_load: float = 0.1, gpu_memory: float = 0.1):
        resources = {'cpu': [*range(os.cpu_count())]}
        gpus = GPUtil.getAvailable(
            limit=100, maxLoad=gpu_load, maxMemory=gpu_memory)
        if len(gpus) > 0:
            resources['gpu'] = gpus
        return resources

    def split(self, num_split: int):
        results = [{} for _ in range(num_split)]
        for key, value in self.resources.items():
            split_size = len(value) // num_split
            for split_i in range(num_split):
                results[split_i][key] = value[
                    split_i * split_size: (split_i + 1) * split_size]
        results = [Resources(r) for r in results]
        return results

    def allocate(self, resource_unit: Dict[str, Union[int, float]]):
        num_split = min([int(len(self.resources[key]) / unit)
                         for key, unit in resource_unit.items()])
        results = [{} for _ in range(num_split)]
        for key, unit in resource_unit.items():
            value = self.resources[key]
            if isinstance(unit, int):
                for split_i in range(num_split):
                    results[split_i][key] = value[
                        split_i * unit: (split_i + 1) * unit]
            else:
                for split_i, v in zip(range(num_split), cycle(value)):
                    results[split_i][key] = [v]
        results = [Resources(r) for r in results]
        return results

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, ', '.join(
            ['%s=%s' % (k, v) for k, v in self.resources.items()]))
