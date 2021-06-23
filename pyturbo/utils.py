import logging
from typing import Iterable, Optional

import psutil
from tqdm.autonotebook import tqdm

from .runtime import Options, mp


def progressbar(iterable: Optional[Iterable] = None, desc: Optional[str] = None,
                total: Optional[int] = None, *, silent: bool = False,
                **kwargs):
    if not silent:
        identity = mp.current_process()._identity
        if 'position' not in kwargs and len(identity):
            kwargs['position'] = identity[0]
        return tqdm(iterable, desc, total, dynamic_ncols=True, **kwargs)
    return iterable


def get_logger(name: str, level: Optional[int] = None,
               log_file: Optional[str] = None):
    if level is None:
        level = logging.INFO if not Options.print_debug_log else logging.DEBUG
    if log_file is None:
        log_file = Options.log_file
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if len(logger.handlers) > 0:
        return logger
    formatter = logging.Formatter(
        '%(asctime)s %(name)s %(levelname)s %(message)s')
    handlers = [logging.StreamHandler()]
    if log_file is not None:
        handlers.append(logging.FileHandler(log_file))
    for handler in handlers:
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def process_map(worker, jobs, num_worker=None, **kwargs):
    if num_worker is None:
        num_worker = len(psutil.Process().cpu_affinity())
    with mp.Pool(num_worker) as pool:
        yield from progressbar(
            pool.imap_unordered(worker, jobs), total=len(jobs), **kwargs)
