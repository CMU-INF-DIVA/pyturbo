import resource

try:
    import torch.multiprocessing as mp
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (1048576, 1048576))
    except:
        mp.set_sharing_strategy('file_system')
except:
    import multiprocessing as mp

mp = mp.get_context('spawn')
