# Options

## Available options

|        Option        |                  True                   |                 False (default)                 |
| :------------------: | :-------------------------------------: | :---------------------------------------------: |
|   raise_exception    |             raise exception             |            log exception and ignore             |
| single_sync_pipeline | run in SyncPipeline in a single process |   run in AsyncPipeline in multiple processes    |
|   no_progress_bar    |     print log at job start and end      | show progress bar of jobs and tasks of each job |
|   print_debug_log    |  print all logs including DEBUG level   |       print logs of INFO level and above        |

## Control

- Environment variable `PYTURBO_OPTIONS`. For example,

```sh
PYTURBO_DEV="raise_exception single_sync_pipeline" python xxx.py
```

- `pyturbo.Options`. For example,

```python
from pyturbo import Options
Options.raise_exception = True
```
