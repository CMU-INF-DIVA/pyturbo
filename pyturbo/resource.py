from typing import Any, List, Union


class Resource(object):

    '''
    One type of resource, such as GPU.
    '''

    def __init__(self, name: str, values: List[Any],
                 unit_size: Union[int, float]):
        self.name = name
        self.values = values
        self.unit_size = unit_size
        self.allocation = self._allocate()

    def _allocate(self):
        if isinstance(self.unit_size, int):
            allocation = [
                self.values[start: start + self.unit_size]
                for start in range(0, len(self.values), self.unit_size)]
        else:
            allocation = [[v] for v in self.values] * int(1 / self.unit_size)
        return allocation

    def __iter__(self):
        for value in self.allocation:
            yield self.name, value

    def __repr__(self):
        return '%s(%s=%s)' % (
            self.__class__.__name__, self.name, self.allocation)
