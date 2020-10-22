from functools import partial
from queue import SimpleQueue
import queue
from threading import Event, Thread
from typing import Any, Iterable, List, Optional

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

    def __init__(self, name: str, task: Task, length: Optional[int] = None,
                 max_retry: int = 1):
        self.name = name
        self.task = task
        self.length = length
        self.retry = max_retry
        self.success = False

    def finish(self, results: Any, pipeline_id: int):
        self.results = results
        self.pipeline_id = pipeline_id
        self.success = True

    def __repr__(self):
        return '%s(%s, len=%d, retry=%d)' % (
            self.__class__.__name__, self.name, self.length, self.retry)


class System(object):

    '''
    A system consists of multiple peer pipelines.
    '''

    def __init__(self, *, pipeline_queue_size: int = 16,
                 resource_scan_args={}, **custom_args):
        self.pipeline_queue_size = pipeline_queue_size
        self.debug_mode = Options.single_sync_pipeline
        self.resources = Resources(**resource_scan_args)
        self.num_pipeline = self.get_num_pipeline(
            self.resources, **custom_args)
        if self.debug_mode:
            self.num_pipeline = 1
            self.pipeline_cls = SyncPipeline
        else:
            self.pipeline_cls = AsyncPipeline
        self.logger = get_logger(repr(self))
        self.logger.info('Available resources: %s', repr(self.resources))
        self.logger.info('Running with Options: %s', repr(Options))
        if self.debug_mode:
            self.logger.info(
                'Debug mode: one SyncPipeline in a single process.')
        else:
            self.logger.info(
                'Production mode: %d AsyncPipelines', self.num_pipeline)
        self.job_queue = SimpleQueue()
        self.result_queue = SimpleQueue()
        self.ending = Event()
        self.job_count, self.result_count = 0, 0
        self.build()

    def get_num_pipeline(self, resources: Resources) -> int:
        return 1

    def get_stages(self, resources: Resources) -> List[Stage]:
        '''
        Define the stages in a pipeline with given resources.
        '''
        raise NotImplementedError

    def get_results(self, job: Job, results_gen: Iterable[Task]) -> Any:
        '''
        Define how to extract final results from output tasks.
        '''
        results = [task.content for task in results_gen]
        return results

    def monit_pipeline(self, pipeline_id: int, *,
                       task_timeout: Optional[int] = None,
                       queue_timeout: Optional[int] = None):
        logger = get_logger(
            '%s(%d)' % (self.pipeline_cls.__name__, pipeline_id))
        job, job_ok = None, True
        pipeline_ok = True
        try:
            while True:
                job = self.job_queue.get()
                if job is None:
                    return
                job_ok = False
                pipeline = self.pipelines[pipeline_id]
                if not pipeline_ok:
                    logger.info('Restarting')
                    pipeline = self.pipeline_fns[pipeline_id]()
                    pipeline.start(queue_timeout)
                    [*pipeline.wait(task_timeout)]
                    self.pipelines[pipeline_id] = pipeline
                    logger.info('Restarted')
                pipeline_ok = False
                try:
                    if Options.no_progress_bar:
                        logger.info('Processing: %s', job)
                    if not self.debug_mode:
                        pipeline.job_queue.put(job.task, timeout=queue_timeout)
                        pipeline.reset(queue_timeout)
                        results_gen = pipeline.wait(task_timeout)
                    else:
                        results_gen = pipeline.run_task(job.task)
                    results_gen = progressbar(
                        results_gen, desc=' Pipeline %d (%s)' % (
                            pipeline_id, job.name),
                        total=job.length, position=pipeline_id, leave=False,
                        silent=Options.no_progress_bar)
                    results = self.get_results(job, results_gen)
                    if self.debug_mode:
                        pipeline.reset(queue_timeout)
                    pipeline_ok = True
                    if results is None:
                        raise ValueError('Empty results from %s' % (job))
                    job.finish(results, pipeline_id)
                    if Options.no_progress_bar:
                        logger.info('Processed: %s', job)
                except Exception as e:
                    if self.ending.is_set():
                        return
                    logger.exception('Failed: %s', job)
                    if Options.raise_exception:
                        raise e
                    if not pipeline_ok:
                        logger.info('Terminating')
                        pipeline.terminate(queue_timeout * 3)
                finally:
                    self.result_queue.put(job, timeout=queue_timeout)
                    job_ok = True
        except Exception as e:
            logger.exception('Dead')
            if not job_ok:
                self.result_queue.put(job, timeout=queue_timeout)
                job_ok = True
            self.result_queue.put(None, timeout=queue_timeout)
            if Options.raise_exception:
                raise e

    def build(self):
        self.pipeline_fns, self.pipelines = [], []
        for pipeline_i, resources in enumerate(
                self.resources.split(self.num_pipeline)):
            stages = self.get_stages(resources)
            self.logger.info('Building pipeline %d: \n\t%s', pipeline_i,
                             '\n\t'.join([repr(s) for s in stages]))
            pipeline_fn = partial(
                self.pipeline_cls, stages, self.pipeline_queue_size)
            self.pipeline_fns.append(pipeline_fn)
            pipeline = pipeline_fn()
            self.pipelines.append(pipeline)
        self.progressbar = None

    def start(self, *, task_timeout: Optional[int] = 600,
              queue_timeout: Optional[int] = 1):
        self.logger.info('Starting')
        for pipeline in self.pipelines:
            pipeline.start(queue_timeout)
        if not self.debug_mode:
            self.monitor_threads = []
            for i in range(self.num_pipeline):
                thread = Thread(
                    target=self.monit_pipeline, args=(i,),
                    kwargs=dict(task_timeout=task_timeout,
                                queue_timeout=queue_timeout), daemon=True)
                thread.start()
                self.monitor_threads.append(thread)
            for pipeline in self.pipelines:
                [*pipeline.wait(task_timeout)]
        self.logger.info('Started')
        if not Options.no_progress_bar:
            self.progressbar = progressbar(
                total=self.job_count, desc='Jobs', position=self.num_pipeline)

    def add_job(self, job: Job, *, timeout: Optional[int] = 1):
        self.job_queue.put(job, timeout=timeout)
        self.job_count += 1
        if not Options.no_progress_bar:
            self.progressbar.total = self.job_count
            self.progressbar.refresh()
        if self.debug_mode:
            self.job_queue.put(None, timeout=timeout)

    def add_jobs(self, jobs: List[Job], *, timeout: Optional[int] = 1):
        for job in jobs:
            self.add_job(job, timeout=timeout)

    def wait_job(self, *, job_timeout: Optional[int] = 3600,
                 queue_timeout: Optional[int] = 1):
        job = None
        if self.debug_mode:
            self.monit_pipeline(0)
        while self.num_pipeline > 0:
            job = self.result_queue.get(timeout=job_timeout)
            if job is None:
                self.num_pipeline -= 1
                continue
            if not job.success:
                if job.retry > 0:
                    job.retry -= 1
                    self.logger.info('Enqueue for retry: %s', job)
                    self.job_queue.put(job, timeout=queue_timeout)
                    continue
                self.logger.warn('Failed: %s', job)
            break
        else:
            raise ValueError('All pipelines dead')
        self.result_count += 1
        if not Options.no_progress_bar:
            self.progressbar.update()
        else:
            self.logger.info('Jobs processed / total: %d / %d' % (
                self.result_count, self.job_count))
        return job

    def wait_jobs(self, num_jobs: int, *, job_timeout: Optional[int] = 3600,
                  queue_timeout: Optional[int] = 1):
        for _ in range(num_jobs):
            yield self.wait_job(
                job_timeout=job_timeout, queue_timeout=queue_timeout)

    def end(self, *, timeout: Optional[int] = 1):
        self.logger.info('Ending')
        assert self.job_queue.empty(), 'Ending with unprocessed jobs'
        assert self.result_queue.empty(), 'Ending with retrieved results'
        self.ending.set()
        for pipeline in self.pipelines:
            pipeline.end(timeout)
        for pipeline in self.pipelines:
            pipeline.join(timeout)
        if not self.debug_mode:
            for _ in range(self.num_pipeline):
                self.job_queue.put(None, timeout=timeout)
            for thread in self.monitor_threads:
                thread.join(timeout)
        if not Options.no_progress_bar:
            self.progressbar.close()
        self.logger.info('Ended')

    def terminate(self, *, timeout: Optional[int] = 3):
        self.logger.exception('Terminating')
        self.ending.set()
        for pipeline in self.pipelines:
            pipeline.terminate(timeout)
        if not self.debug_mode:
            for _ in range(self.num_pipeline):
                self.job_queue.put(None, timeout=timeout)
            for thread in self.monitor_threads:
                thread.join(timeout)
        if not Options.no_progress_bar:
            self.progressbar.close()
        self.logger.error('Terminated')

    def __repr__(self):
        return '%s(%s)' % (
            self.__class__.__name__,
            'debug' if self.debug_mode else 'x%d' % (self.num_pipeline))
