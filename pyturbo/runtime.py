import faulthandler
import os
import resource
import warnings

try:
    '''
    Prefer Pytorch's multiprocessing module for optimized sharing of Tensor
    '''
    import torch.multiprocessing as mp
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (1048576, 1048576))
    except:
        mp.set_sharing_strategy('file_system')
except ImportError:
    import multiprocessing as mp

mp = mp.get_context('spawn')
faulthandler.enable()

QUEUE_EXCEPTIONS = (BrokenPipeError, ConnectionResetError, EOFError,
                    FileNotFoundError)


class Options(object):

    raise_exception = False
    single_sync_pipeline = False
    no_progress_bar = False
    print_debug_log = False
    log_file = None

    def __init__(self):
        for option in os.environ.get('PYTURBO_OPTIONS', '').split():
            fields = option.split('=')
            if len(fields) > 2:
                warnings.warn('Invalid option format: %s' % (option))
                continue
            name = fields[0]
            value = fields[1] if len(fields) == 2 else True
            if not hasattr(self, name):
                warnings.warn('Unrecognized option: %s' % (option))
                continue
            setattr(self, name, value)

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if name == 'log_file' and value is not None:
            faulthandler.enable(open(value))

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, ', '.join(
            ['%s=%s' % (k, v) for k, v in self.__dict__.items()
             if not k.startswith('_') and v]) or 'None')


Options = Options()
