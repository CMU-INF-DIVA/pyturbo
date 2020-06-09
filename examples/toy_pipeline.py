import time

import sys
sys.path.insert(0, '/home/lijun/workspaces/pyturbo')

from pyturbo import Stage, AsyncPipeline, RegularTask
from pyturbo.utils import progressbar

class ToyStage(Stage):

    def __init__(self, name, num_worker):
        super(ToyStage, self).__init__(name, max_worker=num_worker)

    def process(self, task):
        time.sleep(0.1)
        return task


class Stage1(ToyStage):

    def __init__(self):
        super(Stage1, self).__init__('Stage1', num_worker=1)

    def process(self, task):
        for i in range(1000):
            time.sleep(0.05)
            result = RegularTask(task.content + i, parent_task=task)
            yield result


class Stage2(ToyStage):

    def __init__(self):
        super(Stage2, self).__init__('Stage2', num_worker=4)


class Stage3(ToyStage):

    def __init__(self):
        super(Stage3, self).__init__('Stage3', num_worker=2)


class Stage4(ToyStage):

    def __init__(self):
        super(Stage4, self).__init__('Stage4', num_worker=1)


def main():
    pipeline = AsyncPipeline([Stage1(), Stage2(), Stage3(), Stage4()])
    for v in range(0, 5000, 1000):
        task = RegularTask(v)
        pipeline.job_queue.put(task)
    for r in progressbar(range(5000)):
        pipeline.result_queue.get()

if __name__ == "__main__":
    main()
