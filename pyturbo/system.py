from functools import partial
from queue import Queue
from threading import Lock, Thread
from typing import Any, Iterable, List, Union

from .pipeline import AsyncPipeline, SyncPipeline
from .resource import Resources
from .runtime import Options
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

    def __init__(self, *, job_queue_size: int = -1,
                 pipeline_job_queue_size: int = 32,
                 resource_scan_args={}, pipeline_build_args={}):
        self.debug_mode = Options.single_sync_pipeline
        self.resources = Resources(**resource_scan_args)
        if self.debug_mode:
            self.num_pipeline = 1
            self.pipeline_fn = SyncPipeline
        else:
            self.num_pipeline = self.get_num_pipeline(self.resources)
            self.pipeline_fn = partial(
                AsyncPipeline, job_queue_size=pipeline_job_queue_size)
        self.logger = get_logger(repr(self))
        self.logger.info('Available resources: %s', repr(self.resources))
        self.logger.info('Running with Options: %s', repr(Options))
        if self.debug_mode:
            self.logger.info(
                'Debug mode: one SyncPipeline in a single process.')
        else:
            self.logger.info(
                'Production mode: %d AsyncPipelines', self.num_pipeline)
        self.job_queue = Queue(job_queue_size)
        self.result_queue = Queue()
        self.job_count, self.result_count = 0, 0
        self.thread_lock = Lock()
        self.build(**pipeline_build_args)

    def get_num_pipeline(self, resources: Resources) -> int:
        return 1

    def get_stages(self, resources: Resources) -> List[Stage]:
        '''
        Define the stages in a pipeline with given resources.
        '''
        raise NotImplementedError

    def get_results(self, job: Job, results_gen: Iterable) -> List[Any]:
        '''
        Define how to extract final results from output tasks.
        '''
        results = []
        for task in results_gen:
            results.append(task.content)
        return results

    def monit_pipeline(self, pipeline_id: int):
        pipeline = self.pipelines[pipeline_id]
        while True:
            job = self.job_queue.get()
            if job is None:
                return
            if Options.no_progress_bar:
                self.logger.info(
                    ' Pipeline %d: processing job: %s', pipeline_id, job.name)
            if not self.debug_mode:
                pipeline.job_queue.put(job.task)
                pipeline.reset()
                results_gen = pipeline.wait()
            else:
                results_gen = pipeline.run_task(job.task)
            results_gen = progressbar(
                results_gen, desc=' Pipeline %d (%s)' % (
                    pipeline_id, job.name),
                total=job.length, position=pipeline_id, leave=False,
                silent=Options.no_progress_bar)
            results = self.get_results(job, results_gen)
            if isinstance(pipeline, SyncPipeline):
                pipeline.reset()
            job.finish(results)
            self.result_queue.put(job)
            with self.thread_lock:
                self.result_count += 1
                if not Options.no_progress_bar:
                    self.progressbar.update()
            if Options.no_progress_bar:
                self.logger.info(
                    ' Pipeline %d: processed job: %s', pipeline_id, job.name)
                self.logger.info('Jobs processed / total: %d / %d' % (
                    self.result_count, self.job_count))

    def build(self, **pipeline_args):
        self.pipelines = []
        for pipeline_i, resources in enumerate(
                self.resources.split(self.num_pipeline)):
            stages = self.get_stages(resources)
            pipeline = self.pipeline_fn(stages, **pipeline_args)
            self.pipelines.append(pipeline)
            self.logger.info('Building pipeline %d: \n\t%s', pipeline_i,
                             '\n\t'.join([repr(s) for s in stages]))
        if not self.debug_mode:
            self.monitor_threads = [
                Thread(target=self.monit_pipeline, args=(i,))
                for i in range(self.num_pipeline)]
        self.progressbar = None

    def start(self):
        self.logger.info('Starting system')
        if not Options.no_progress_bar:
            self.progressbar = progressbar(
                total=self.job_count, desc='Jobs', position=self.num_pipeline)
        for pipeline in self.pipelines:
            pipeline.start()
        if not self.debug_mode:
            for thread in self.monitor_threads:
                thread.start()

    def add_job(self, job: Job):
        self.job_queue.put(job)
        self.job_count += 1
        if not Options.no_progress_bar:
            self.progressbar.total = self.job_count
            self.progressbar.refresh()
        if self.debug_mode:
            self.job_queue.put(None)
            self.monit_pipeline(0)

    def end(self):
        self.logger.info('Ending system')
        for pipeline in self.pipelines:
            pipeline.end()
        for pipeline in self.pipelines:
            pipeline.join()
        for _ in range(self.num_pipeline):
            self.job_queue.put(None)
        if not self.debug_mode:
            for thread in self.monitor_threads:
                thread.join()
        if not Options.no_progress_bar:
            self.progressbar.close()

    def terminate(self):
        self.logger.info('Terminating system')
        for pipeline in self.pipelines:
            pipeline.terminate()
        if not self.debug_mode:
            for thread in self.monitor_threads:
                thread.terminate()
        if not Options.no_progress_bar:
            self.progressbar.close()

    def __repr__(self):
        return '%s(%s)' % (
            self.__class__.__name__,
            'debug' if self.debug_mode else 'x%d' % (self.num_pipeline))
