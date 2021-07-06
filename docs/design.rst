Design
======

Colmena is a library on which you can build applications to steer
ensembles of simulations running on HPC resources.
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

.. image:: _static/overview.svg
    :height: 200px
    :align: center

"Thinker": Active Learning Agent
++++++++++++++++++++++++++++++++

The "Thinker" process is responsible for generating tasks to send to the task server.
Colmena supports many different kinds of task generation methods each with
different concurrency and optimization performance tradeoffs.
For example, one could develop a batch optimization algorithm
that waits for every simulation in a batch to complete before
sending deciding new simulations or a streaming optimization
tool that continuously maintains a queue of new computations.

"Doer": task server
+++++++++++++++++++

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
"Thinker" applications make requests to the task server for computations
and receive the results in no particular order.

Implementation
--------------

Our current implementation of Colmena is based around a Parsl workflow
engine to manage computations and Redis for asynchronous communication.

.. image:: _static/implementation.svg
    :align: center

Client
++++++

The "Thinker" process, which we refer to as the client,
in a Colmena application is custom software developed to
decide which tasks are run.

The client communicates by either writing *task requests* to or reading *results* from
Redis queues.
Tasks and results are communicated as JSON objects and contain the inputs to a task,
the outputs of the task, and a variety of profiling data (e.g., task runtime,
time inputs received by task server).
We provide a Python API for the message format, :class:`colmena.models.Result`,
which provides utility operations for tasks that include accessing the positional
or keyword arguments for a task and serializing the inputs and results.

Task Server
+++++++++++

We implement a task server based on `Parsl <https://parsl-project.org>`_.
Parsl provides a model of distributed computing in Python that meshes well with
Python's native :mod:`concurrent.futures` module and allows for users to express complex
workflows in Python.
We create :class:`parsl.app.PythonApp` for each of the methods available in the task server,
which allows us to use them as part of Parsl workflows and execute them on distributed resources.

The :class:`colmena.task_server.ParslMethodServer` itself is a multi-process, multi-threaded Python application:

1. *Intake Thread*: The intake thread reads task requests from the input Redis queue(s), deserializes
   them and submits the appropriate tasks to Parsl. Submitting a task to Parsl involves calling
   the ``PythonApp`` for a certain method defined in the input specification and creating a workflow
   such that the result from the method will be sent back to the queue.
2. *Parsl Threads*: Parsl is a multi-threaded application which handles launching and communicating
   with worker processes. These workers can reside on other systems (e.g., compute or launch nodes on HPC).
3. *Output Process*: Each task is defined as a two-step workflow in Parsl: "perform task" followed by
   "output results". The "output results" step in the workflow is performed on a collection of porcesses
   managed by Parsl.
4. *Error Collection Thread*: Tasks which fail to run are periodically sent back to the client
   over the Redis queue. The Error thread captures errors reported from Parsl and adds them to the Redis queue.

Communication
+++++++++++++

Communication between the client and task server occurs using Redis queues.
Each Colmena application has at least two queues: an "inputs" queue for task
requests and a "results" queue for results.
By default, operations will pull from these two default queues.
Colmena also supports creating "topical" queues for tasks for special categories
(e.g., tasks associated with active learning vs simulations).
Client processes can filter to only read from these topical queues, which simplifies
breaking a "Thinker" application in multiple sub-agents.

The :mod:`colmena.redis.queue` module contains utility classes for interacting with these redis queues.
For example, the :class:`colmena.redis.queue.ClientQueues` provides a wrapper for use by a client process.
One utility operation, ``send_inputs``, wraps task descriptions in the ``Result`` class,
serializes the inputs, and pushes them to the result queue.
There is a corresponding, ``get_result``, operation which pulls a result from the result queue
and deserializes the result.
Each of these operations can be supplied with a topic to either send inputs with a
designated topic or to receive only a result with a certain topic.

There is a corresponding queue wrapper for the task server, :class:`colmena.redis.queue.MethodServerQueues`,
that provides the matching operations to the ``ClientQueues``.
Both need to be created to point to the same Redis server and have the same list of topic names,
and Colmena provides a :meth:`colmena.redis.queue.make_queue_pairs` to generate a matched
set of queues at the beginning of an application.

Life-Cycle of a Task
--------------------

.. TODO (wardlt): Make a figure to illustrate the task routing

We describe the life-cycle of a task to illustrate how all of the components of Colmena work together
by illustrating a typical :class:`colmena.models.Result` object.

.. code-block:: json
    :linenos:

    {
        "inputs": [[1, 1], {"operator": "add"}],
        "serialization_method": "pickle",
        "method": "reduce",
        "value": 2,
        "success": true,
        "time_created": 1593498015.132477,
        "time_input_received": 1593498015.13357,
        "time_compute_started": 1593498018.856764,
        "time_result_sent": 1593498018.858268,
        "time_result_received": 1593498018.860002,
        "time_running": 1.8e-05,
        "time_serialize_inputs": 4.07e-05,
        "time_deserialize_inputs": 4.28-05,
        "time_serialize_results": 3.32e-05,
        "time_deserialize_results": 3.30e-05,
    }

**Launching Tasks**: A client creates a task request at ``time_created`` and adds the the input
specification (``method`` and ``inputs``) to an "outbound" Redis queue. The task request is formatted
in the JSON format defined above with only the ``method``, ``inputs`` and ``time_created`` fields
populated. The task inputs are then serialized (``time_serialize_inputs``) and send using
the Redis Queue to the task server.
The serialization method is communicated along with the inputs.

**Task Routing**: The task server reads the task request from the outbound queue at ``time_input_received``
and submits the task to the distributed workflow engine.
The method definitions in the task server denote on which resources they can run,
and Parsl chooses when and to which resource to submit tasks.

**Computation**: A Parsl worker starts a task at ``time_compute_started``.
The task inputs are deserialized (``time_deserialize_inputs``),
the requested work is executed (``time_running``),
and the results serialized (``time_serialize_results``).

**Result Communication**: The task server adds the result to the task specification (``value``) and
sends it back to the client in an "inbound" queue at (``time_result_sent``).

**Result Retrieval**: The client retrieves the message from the inbound queue.
The result is deserialized (``time_deserialize_result``) and returned
back to the client at ``time_result_received``.

The overall efficiency of the task system can be approximated by comparing the ``time_running``, which
denotes the actual time spent executing the task on the workers, to the difference between the ``time_created``
and ``time_returned`` (i.e., the round-trip time).
Comparing round-trip time and ``time_running`` captures both the overhead of the system and any time
waiting in a queue for other tasks to complete and must be viewed carefully.

The overhead specific to Colmena (i.e., and not Parsl) can be measured by assessing the communication time
for the Redis queues.
For example, the inbound queue can be assessed by comparing the ``time_created`` and ``time_input_received``.
The communication times for Parsl can be measured only when the queue length is negligible
through the differences between ``time_inputs_received`` and ``time_compute_started``.
The communication times related to serialization are also stored (e.g., ``time_serialize_result``).
