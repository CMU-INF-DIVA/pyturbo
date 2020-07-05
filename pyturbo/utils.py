import logging
import os
from functools import partial

from tqdm.autonotebook import tqdm

from .runtime import debug

progressbar = partial(tqdm, dynamic_ncols=True)


def get_logger(name, level=None, log_file=None):
    if level is None:
        level = logging.INFO if not debug else logging.DEBUG
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
