# Development Modes

## Available modes

- exception: raise exceptions rather than just logging
- pipeline: run in SyncPipeline in a single process
- log: print detailed logs

## Control

- Environment variable `PYTURBO_DEV`. For example,

```sh
PYTURBO_DEV="exception pipeline" python xxx.py
```

- `pyturbo.DevModes`. For example,

```python
from pyturbo import DevModes
# DevModes is a Python set object
DevModes.add('exception')  # turn on one
DevModes.update(['exception', 'pipeline'])  # turn on several
DevModes.remove('pipeline')  # turn off one
```
