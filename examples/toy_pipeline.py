import sys
import time

sys.path.insert(0, '/home/lijun/workspaces/pyturbo')
from pyturbo import AsyncPipeline, ReorderStage, Stage, Task
from pyturbo.utils import progressbar


class Stage1(Stage):

    def __init__(self):
        super(Stage1, self).__init__(max_worker=1)

    def process(self, task):
        for i in range(1000):
            time.sleep(0.01)
            result = Task(task.content + i, parent_task=task)
            yield result


class Stage2(Stage):

    def __init__(self):
        super(Stage2, self).__init__(max_worker=4)

    def process(self, task):
        time.sleep(0.01)
        return task


class Stage3(Stage):

    def __init__(self):
        super(Stage3, self).__init__(max_worker=2)

    def process(self, task):
        time.sleep(0.01)
        return task


class Stage4(ReorderStage):

    def get_sequence_id(self, task):
        return task.content

    def process(self, task):
        time.sleep(0.01)
        return task


def main():
    pipeline = AsyncPipeline([Stage1(), Stage2(), Stage3(), Stage4()])
    for v in range(0, 2000, 1000):
        task = Task(v)
        pipeline.job_queue.put(task)
    for r in progressbar(range(2000)):
        d = pipeline.result_queue.get()
        assert d.content == r
    pipeline.reset()
    for v in range(2000, 4000, 1000):
        task = Task(v)
        pipeline.job_queue.put(task)
    for r in progressbar(range(2000)):
        d = pipeline.result_queue.get()
        assert d.content == r + 2000
    pipeline.end()
    pipeline.join()


if __name__ == "__main__":
    main()
