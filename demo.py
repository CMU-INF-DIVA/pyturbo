import random
import sys
import time

from pyturbo import Job, ReorderStage, Stage, System, Task


class Stage1(Stage):

    '''
    Take in two integers x and y, generate x ~ x + y
    '''

    def __init__(self, resources):
        super(Stage1, self).__init__(resources, max_worker=1)

    def process(self, task):
        x, y = task.content
        for i in range(y):
            time.sleep(0.02)
            content = (x + i, x)
            result = Task(content, {'i': i}, parent_task=task)
            yield result


class Stage2(Stage):

    '''
    x -> x * 7
    '''

    def __init__(self, resources):
        super(Stage2, self).__init__(resources, max_worker=4)

    def process(self, task):
        time.sleep(0.005)
        x, x0 = task.content
        result = (x * 7, x0)
        task = Task(result, parent_task=task)
        return task


class Stage3(Stage):

    '''
    x -> int(x / 7)
    '''

    def __init__(self, resources):
        super(Stage3, self).__init__(resources, max_worker=2)

    def process(self, task):
        time.sleep(0.01)
        x, x0 = task.content
        result = (int(x / 7), x0)
        task = Task(result, parent_task=task)
        return task


class Stage4(ReorderStage):

    '''
    x -> -x
    '''

    def __init__(self, resources):
        super(Stage4, self).__init__(resources)

    def get_sequence_id(self, task):
        x, x0 = task.content
        return x - x0

    def process(self, task):
        time.sleep(0.01)
        x = task.content[0]
        result = -x
        task = Task(result, parent_task=task)
        return task


class ToySystem(System):

    '''
    (x, y) -> [*range(-x, -x - y, -1)]
    '''

    def get_stages(self, resources):
        stages = [Stage1, Stage2, Stage3, Stage4]
        resources = resources.split(len(stages))
        stages = [s(r) for s, r in zip(stages, resources)]
        return stages

    def get_results(self, results_gen):
        results = [task.content for task in results_gen]
        return results


def main(num_pipeline=4, n_job=9, **system_args):
    system = ToySystem(num_pipeline, **system_args)
    system.start()
    for _ in range(n_job):
        x = random.randint(0, 9000)
        y = random.randint(200, 400)
        task = Task((x, y), {'x': x, 'y': y})
        name = '%d_%d' % (x, y)
        job = Job(name, task, y)
        system.add_job(job)
    for _ in range(9):
        job = system.result_queue.get()
        x, y = job.task.content
        assert job.results == [*range(-x, -x - y, -1)]
    system.end()


if __name__ == "__main__":
    main()
    main(debug=True)
