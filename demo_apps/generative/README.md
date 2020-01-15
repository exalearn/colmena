# Generative AI Workflow

This demo illustrates a workflow for optimization that uses multiple machine learning tools:

- _Generative_: Create similar entries to given points
- _Supervised_: Estimate the output of functions
- _Active_: Identify which inputs would be the most-value training examples

The tool is implemented using a single MethodServer that runs all four of the tasks.
The advantage of running a single method server is that there is only one set of
queues to manage and a simpler method server implementation.
The disadvantage of the single method server is that it leads to a more complex client 
because there are no guarantees on the method at the end of queue. 
For that reason, our implementation submits only one task the method server at a time.

At present, retraining all machine-learning models is handled on the resources running
the ``Thinker`` class.

## Running 

This example uses a custom class, ``Generator``, which must be installed on the Python path 
for its definition to be accessible to the worker processes.
Install it by calling ``pip install -e .``, which will put the ``generator_demo`` module
on your pass.

First launch a Redis server, then call `python generative.py`.
 
