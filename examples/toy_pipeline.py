import time

import sys
sys.path.insert(0, '/home/lijun/workspaces/pyturbo')

from pyturbo import Stage, AsyncPipeline, RegularTask
from pyturbo.utils import progressbar

class ToyStage(Stage):

    def __init__(self, num_worker):
        super(ToyStage, self).__init__(max_worker=num_worker)

    def process(self, task):
        time.sleep(0.01)
        return task


class Stage1(ToyStage):

    def __init__(self):
        super(Stage1, self).__init__(1)

    def process(self, task):
        for i in range(1000):
            time.sleep(0.01)
            result = RegularTask(task.content + i, parent_task=task)
            yield result


class Stage2(ToyStage):

    def __init__(self):
        super(Stage2, self).__init__(4)


class Stage3(ToyStage):

    def __init__(self):
        super(Stage3, self).__init__(2)


class Stage4(ToyStage):

    def __init__(self):
        super(Stage4, self).__init__(1)


def main():
    pipeline = AsyncPipeline([Stage1(), Stage2(), Stage3(), Stage4()])
    for v in range(0, 2000, 1000):
        task = RegularTask(v)
        pipeline.job_queue.put(task)
    for r in progressbar(range(2000)):
        pipeline.result_queue.get()
    pipeline.reset()
    for v in range(2000, 4000, 1000):
        task = RegularTask(v)
        pipeline.job_queue.put(task)
    for r in progressbar(range(2000)):
        pipeline.result_queue.get()
    pipeline.end()
    pipeline.join()

if __name__ == "__main__":
    main()
