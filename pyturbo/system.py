from functools import partial
from queue import Queue
from threading import Thread
from typing import List, Any, Union

from .pipeline import AsyncPipeline, SyncPipeline
from .resource import Resources
from .stage import Stage
from .task import Task
from .utils import get_logger, progressbar


class Job(object):

    def __init__(self, name: str, task: Task, length: Union[None, int] = None):
        self.name = name
        self.task = task
        self.length = length

    def finish(self, results):
        self.results = results


class System(object):

    def __init__(self, num_pipeline: int = 1, debug=False,
                 show_progress=True,
                 job_queue_size: int = -1,
                 pipeline_job_queue_size: int = 32,
                 **resource_scan_args):
        self.logger = get_logger(repr(self))
        self.num_pipeline = num_pipeline
        self.pipeline_fn = partial(
            AsyncPipeline, job_queue_size=pipeline_job_queue_size)
        if debug:
            self.logger.info('Debug mode: one SyncPipeline in single process.')
            self.num_pipeline = 1
            self.pipeline_fn = SyncPipeline
        self.show_progress = show_progress
        self.job_queue = Queue(job_queue_size)
        self.resources = Resources(**resource_scan_args)
        self.result_queue = Queue()
        self.build()

    def get_stages(self, resources) -> List[Stage]:
        '''
        Define the stages in a pipeline with given resources.
        '''
        raise NotImplementedError

    def get_results(self, results_gen) -> List[Any]:
        '''
        Define how to extract final results from output tasks.
        '''
        raise NotImplementedError

    def monitor(self, pipeline_id):
        pipeline = self.pipelines[pipeline_id]
        while True:
            job = self.job_queue.get()
            if job is None:
                return
            pipeline.job_queue.put(job.task)
            pipeline.reset()
            results_gen = pipeline.wait()
            if self.show_progress:
                results_gen = progressbar(
                    results_gen, desc=' P%d(%s)' % (pipeline_id, job.name),
                    total=job.length, position=pipeline_id)
            results = self.get_results(results_gen)
            job.finish(results)
            self.result_queue.put(job)

    def build(self, **pipeline_args):
        self.pipelines = []
        for resources in self.resources.split(self.num_pipeline):
            stages = self.get_stages(resources)
            pipeline = self.pipeline_fn(stages, **pipeline_args)
            self.pipelines.append(pipeline)
        self.monitor_threads = [Thread(target=self.monitor, args=(i,))
                                for i in range(self.num_pipeline)]

    def start(self):
        for pipeline in self.pipelines:
            pipeline.start()
        for thread in self.monitor_threads:
            thread.start()

    def end(self):
        for pipeline in self.pipelines:
            pipeline.end()
        for pipeline in self.pipelines:
            pipeline.join()
        for _ in range(self.num_pipeline):
            self.job_queue.put(None)
        for thread in self.monitor_threads:
            thread.join()

    def terminate(self):
        for pipeline in self.pipelines:
            pipeline.terminate()
        for thread in self.monitor_threads:
            thread.terminate()

    def __repr__(self):
        return self.__class__.__name__
