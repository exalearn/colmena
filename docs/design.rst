Design
======

Colmena is a library on which you can build other applications and
is not a tool for steering ensembles of simulations on its own.
This portion of the documentation discuss the components of Colmena
and illustrate how they work together.

Key Concepts
------------

Applications based on Colmena are typically composed of two separate processes:
a "Thinker" application and a "Doer" application.
The "Thinker" application determines the computations to perform and
communicates specifications to the "Doer."
These computations can consist of both the target simulations to be
performed (e.g., a DFT code) *and* computations used to decide which
simulation to perform next (e.g., training and inference of a machine-learned
surrogate model).

.. TODO: Make a figure

"Thinker": Active Learning Agent
++++++++++++++++++++++++++++++++

The "Thinker" process is responsible for generating tasks to send to the Method server.
Colmena supports many different kinds of task generation methods each with
different concurrency and optimization performance tradeoffs.
For example, one could develop a batch optimization algorithm
that waits for every simulation in a batch to complete before
sending deciding new simulations or a streaming optimization
tool that continuously maintains a queue of new computations.

"Doer": Method Server
+++++++++++++++++++++

The "Doer" server accepts tasks specification, deploys tasks on remote services
and sends results back to the Thinker agent(s).
The "Doer" process also stores information about available
methods including task descriptions (e.g., a DAG representation of a workflow)
and supports updating that task descriptions on request
(e.g., updating to a latest version of a machine learning model).

Communication
+++++++++++++

Communication between the "Thinker" and "Doer" is asynchronous
and follows a very specific pattern.
"Thinker" applications make requests to the method server for computations
and receive the results in no particular order.

.. Need to check my nomenclature with a distributed computing person

Implementation
--------------

Our current implementation of Colmena is based around a Parsl workflow
engine to manage computations and Redis for asynchronous communication.

Method Server
+++++++++++++

TBD
