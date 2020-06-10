import sys
import time
sys.path.insert(0, '/home/lijun/workspaces/pyturbo')

from pyturbo.utils import progressbar
from pyturbo import Job, ReorderStage, Stage, Task, System

class Stage1(Stage):

    def __init__(self, resources):
        super(Stage1, self).__init__(resources, max_worker=1)

    def process(self, task):
        base = task.content
        for i in range(1000):
            time.sleep(0.01)
            content = (base + i, base)
            result = Task(content, parent_task=task)
            yield result


class Stage2(Stage):

    def __init__(self, resources):
        super(Stage2, self).__init__(resources, max_worker=4)

    def process(self, task):
        time.sleep(0.01)
        result = (task.content[0] * 7, task.content[1])
        task = Task(result, parent_task=task)
        return task


class Stage3(Stage):

    def __init__(self, resources):
        super(Stage3, self).__init__(resources, max_worker=2)

    def process(self, task):
        time.sleep(0.005)
        result = (int(task.content[0] / 7), task.content[1])
        task = Task(result, parent_task=task)
        return task


class Stage4(ReorderStage):

    def __init__(self, resources):
        super(Stage4, self).__init__(resources)

    def get_sequence_id(self, task):
        return task.content[1] - task.content[0]

    def process(self, task):
        time.sleep(0.01)
        result = -task.content[0]
        task = Task(result, parent_task=task)
        return task


class ToySystem(System):

    def __init__(self):
        super(ToySystem, self).__init__(num_pipeline=1)

    def get_stages(self, resources):
        resources = resources.split(4)
        stages = [Stage1, Stage2, Stage3, Stage4]
        stages = [s(r) for s, r in zip(stages, resources)]
        return stages

    def get_results(self, results_gen):
        results = [task.content for task in results_gen]
        return results

    



def main():
    system = ToySystem()
    system.start()
    for v in range(0, 2000, 1000):
        task = Task(v)
        job = Job(str(v), task, 1000)
        system.job_queue.put(job)
        job = system.result_queue.get()
        assert job.results == [*range(-job.task.content, -job.task.content - 1000, -1)]
    system.end()
    # for r in progressbar(range(2000)):
    #     d = pipeline.result_queue.get()
    #     assert d.content == r
    # pipeline.reset()
    # for v in range(2000, 4000, 1000):
    #     task = Task(v)
    #     pipeline.job_queue.put(task)
    # for r in progressbar(range(2000)):
    #     d = pipeline.result_queue.get()
    #     assert d.content == r + 2000
    # pipeline.end()
    # pipeline.join()


if __name__ == "__main__":
    main()
