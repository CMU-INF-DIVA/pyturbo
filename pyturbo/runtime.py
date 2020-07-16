import faulthandler
import os
import resource

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


class Options(object):

    def __init__(self):
        self.raise_exception = False
        self.single_sync_pipeline = False
        self.no_progress_bar = False
        self.print_debug_log = False
        for option in os.environ.get('PYTURBO_OPTIONS', '').split():
            if not hasattr(self, option):
                print('Warning: option %s unrecognized.' % (option))
                continue
            setattr(self, option, True)

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, ', '.join(
            ['%s=%s' % (k, v) for k, v in self.__dict__.items()
             if not k.startswith('_') and v]) or 'None')


Options = Options()
