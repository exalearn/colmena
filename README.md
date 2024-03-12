# Colmena

[![CI](https://github.com/exalearn/colmena/actions/workflows/CI.yml/badge.svg)](https://github.com/exalearn/colmena/actions/workflows/CI.yml)
[![Documentation Status](https://readthedocs.org/projects/colmena/badge/?version=latest)](https://colmena.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/colmena.svg)](https://badge.fury.io/py/colmena)
[![Coverage Status](https://coveralls.io/repos/github/exalearn/colmena/badge.svg?branch=master)](https://coveralls.io/github/exalearn/colmena?branch=master)

Colmena simplifies building autonomous applications that steer large campaigns of simulations on supercomputers.

Such "high-throughput" campaigns were, historically, 
guided by humans identifying the which tasks to run next &mdash; a time-consuming process with a high latency between "new data" and "decisions."

Colmena was created to build applications which augment or replace human steering with Artificial Intelligence (AI). 

## Installation

Colmena is available [via PyPI](): `pip install colmena`

Consult our [Installation Guide](https://colmena.readthedocs.io/en/latest/installation.html) for further details.

## Using Colmena

Colmena applications describe a computational campaign in two components: a "Thinker" that picks computations
and a "Doer" which executes them.

Thinkers encode the logic for how to run new calculations as "agents."
Complex strategies are simple to express when you decompose them into simple steps.
For example, a distributed optimizer:

```python
from random import random

from colmena.thinker import BaseThinker, result_processor, task_submitter, ResourceCounter
from colmena.queue import PipeQueues
from colmena.models import Result

# Build queues to connect Thinker and Doer
queues = PipeQueues()


class Thinker(BaseThinker):

    def __init__(self, queues, num_workers: int, num_guesses=100):
        super().__init__(queues, ResourceCounter(num_workers))
        self.best_result = None
        self.answer = -10  # A (bad) starting guess
        self.num_guesses = num_guesses

    @task_submitter()
    def submit_task(self):
        """Submit a new guess close to the current best whenever a node is free"""
        self.queues.send_inputs(self.answer - 1 + 2 * random(), method='simulate')

    @result_processor()
    def store_result(self, result: Result):
        """Update best guess whenever a simulation finishes"""
        assert result.success, result.failure_info
        # Update the best result
        if self.best_result is None or result.value > self.best_result:
            self.answer = result.args[0]
            self.best_result = result.value
        self.rec.release()  # Mark that a node is now free

        # Determine if we are done
        self.num_guesses -= 1
        if self.num_guesses <= 0:
            self.done.set()


thinker = Thinker(queues, 8)
```

Doers describe the types of computations and available compute resources.
Colmena provides Task Servers backed by several workflow engines, such as those from the [ExaWorks project](https://exaworks.org/).
Building one using Parsl requires only that your computations are expressed as Python functions:

```python
from parsl.configs.htex_local import config  # Configuration to run locally
from colmena.task_server.parsl import ParslTaskServer

# Define your function
def simulate(x: float) -> float:
    return - x ** 2 + 4

# Make the Doer
doer = ParslTaskServer([simulate], queues, config)
```

Once these are defined, launching the application involves starting both

```python

# Launch the Thinker and doer
doer.start()
thinker.start()

# Wait until it finishes
thinker.join()
queues.send_kill_signal()  # Stop the doer

# Done!
print(f'Answer: f({thinker.answer:.2f}) = {thinker.best_result:.2f}')
```

### Tutorials

Visit the [Quickstart](https://colmena.readthedocs.io/en/latest/quickstart.html) to learn to build a full application.

### More Examples

See the [`demo_apps`](./demo_apps) to see a variety of ways to use Colmena.

## Learning More

Our [Read-the-Docs](https://colmena.readthedocs.io/en/latest/) provides the most up-to-date information about Colmena.

You can also learn more about Colmena in the papers we published about it:

- Ward et al. "Colmena: Scalable Machine-Learning-Based Steering of Ensemble Simulations for High Performance Computing".
  _2021 IEEE/ACM Workshop on Machine Learning in High Performance Computing Environments (MLHPC)_
  [[doi](https://doi.org/10.1109/MLHPC54614.2021.00007)] [[ArXiv](https://arxiv.org/abs/2110.02827)]
  [[slides](https://www.researchgate.net/publication/357777568)]
  [[YouTube](https://youtu.be/-3KnbJcm-tQ)]

## Acknowledgements 

This project was supported in part by the Exascale Computing Project (17-SC-20-SC) of the U.S. Department of Energy (DOE) and by DOE’s Advanced Scientific Research Office (ASCR) under contract DE-AC02-06CH11357.
