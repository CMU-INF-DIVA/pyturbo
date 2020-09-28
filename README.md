# Pyturbo Package

[![PyPI version](https://badge.fury.io/py/py-turbo.svg)](https://badge.fury.io/py/py-turbo)
[![Downloads](https://pepy.tech/badge/py-turbo)](https://pepy.tech/project/py-turbo)
![Publish to PyPI](https://github.com/CMU-INF-DIVA/pyturbo/workflows/Publish%20to%20PyPI/badge.svg)

Author: Lijun Yu

Email: lijun@lj-y.com

A pipeline system for efficient execution.

## Installation

```sh
pip install py-turbo
```

## Introduction

`Pyturbo` utilizes multiple level of abstract to efficiently execute parallel tasks.

* Worker: a process.
* Stage: a group of peer workers processing the same type of tasks.
* Task: a data unit transferred between stages. At each stage, a task is processed by one worker and will result in one or multiple downstream tasks.
* Pipeline: a set of sequential stages.
* Job: a data unit for a pipeline, typically a wrapped task for the first stage.
* Result: output of a job processed by one pipeline, typically a set of output tasks from the last stage.
* System: a set of peer pipelines processing the same type of jobs.

![abstract.png](https://github.com/CMU-INF-DIVA/pyturbo/raw/master/docs/abstract.png)

## Get Started

```python
from pyturbo import ReorderStage, Stage, System

class Stage1(Stage): # Define a stage

    def __init__(self, resources):
        ... # Optional: set resources and number of workers

    def process(self, task):
        ... # Process function for each worker process. Returns one or a series of downstream tasks.

... # Repeat for Stage2, Stage3

class Stage4(ReorderStage): # Define a reorder stage, typically for the final stage

    def get_sequence_id(self, task):
        ... # Return the order of each task. Start from 0.

    def process(self, task):
        ...

class MySystem(System):

    def get_stages(self, resources):
        ... # Define the stages in a pipeline with given resources.

    def get_results(self, results_gen):
        ... # Define how to extract final results from output tasks.

def main():
    system = MySystem(num_pipeline) # Set debug=True to run in a single process
    system.start() # Build and start system
    system.add_job(...) # Submit one job
    finished_job = system.result_queue.get() # Wait for result
    system.end() # End system
```

## Options

See [options.md](https://github.com/CMU-INF-DIVA/pyturbo/blob/master/docs/options.md)

## Demo

![abstract.png](https://github.com/CMU-INF-DIVA/pyturbo/raw/master/docs/demo.gif)

See [demo.py](demo.py) for an example implementation.

## Version History

See [version.md](https://github.com/CMU-INF-DIVA/pyturbo/blob/master/docs/version.md).
