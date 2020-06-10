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

    '''
    A job for the system. A job is a wrapper over a task for the first stage.
    '''

    def __init__(self, name: str, task: Task, length: Union[None, int] = None):
        self.name = name
        self.task = task
        self.length = length

    def finish(self, results):
        self.results = results


class System(object):

    '''
    A system consists of multiple peer pipelines.
    '''

    def __init__(self, num_pipeline: int = 1, debug=False,
                 show_progress=True,
                 job_queue_size: int = -1,
                 pipeline_job_queue_size: int = 32,
                 resource_scan_args={}, pipeline_build_args={}):
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
        self.logger.info('Available resources: %s', repr(self.resources))
        self.result_queue = Queue()
        self.job_count = 0
        self.build(**pipeline_build_args)

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

    def monit_pipeline(self, pipeline_id):
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
                    results_gen,
                    desc=' Pipeline-%d(%s)' % (pipeline_id, job.name),
                    total=job.length, position=pipeline_id, leave=False)
            results = self.get_results(results_gen)
            job.finish(results)
            self.result_queue.put(job)
            self.progressbar.update()

    def build(self, **pipeline_args):
        self.pipelines = []
        for pipeline_i, resources in enumerate(
                self.resources.split(self.num_pipeline)):
            stages = self.get_stages(resources)
            pipeline = self.pipeline_fn(stages, **pipeline_args)
            self.pipelines.append(pipeline)
            self.logger.info('Building pipeline %d: \n\t%s', pipeline_i,
                             '\n\t'.join([repr(s) for s in stages]))
        self.monitor_threads = [Thread(target=self.monit_pipeline, args=(i,))
                                for i in range(self.num_pipeline)]
        self.progressbar = None

    def start(self):
        self.logger.info('Starting system')
        self.progressbar = progressbar(
            total=self.job_count, desc='Jobs', position=self.num_pipeline)
        for pipeline in self.pipelines:
            pipeline.start()
        for thread in self.monitor_threads:
            thread.start()

    def add_job(self, job):
        self.job_queue.put(job)
        self.job_count += 1
        self.progressbar.total = self.job_count
        self.progressbar.refresh()

    def end(self):
        for pipeline in self.pipelines:
            pipeline.end()
        for pipeline in self.pipelines:
            pipeline.join()
        for _ in range(self.num_pipeline):
            self.job_queue.put(None)
        for thread in self.monitor_threads:
            thread.join()
        self.progressbar.close()

    def terminate(self):
        for pipeline in self.pipelines:
            pipeline.terminate()
        for thread in self.monitor_threads:
            thread.terminate()
        self.progressbar.close()

    def __repr__(self):
        return self.__class__.__name__
