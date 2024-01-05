Design
======

Colmena is a library for building applications that steer
ensembles of simulations running on distributed computing resources.

Key Concepts
------------

Applications based on Colmena have two parts: a "Thinker" and "Doer".
The Thinker determines which computations to perform and
delegates them to the Doer.

.. image:: _static/overview.svg
    :height: 200px
    :align: center

"Thinker": Planning Agent
+++++++++++++++++++++++++

The "Thinker" defines the strategy for a computational campaign.
The strategy is expressed by a series of "agents" that identify
which computations to run and adapt to their results.
As `demonstrated in our optimization examples <how-to#creating-a-thinker-application>`_,
complex strategies are simple if broken into many agents.


"Doer": Task Server
+++++++++++++++++++

The "Doer" server accepts tasks specification from the Thinker,
deploys tasks on remote services
and sends results back to the Thinker.
Doers are interfaces to workflow engines, such as `Parsl <https://parsl-project.org>`_
or `Globus Compute <https://funcx.org/>`_.

Implementation
--------------

The Thinker and Doer from Colmena run as separate Python processes that interact over queues.

.. image:: _static/implementation.svg
    :align: center

Client
++++++

The "Thinker" process is a Python program that runs a separate thread for each agent.

Agents are functions that define which computations to run by sending *task requests*
to a task server or reading *results* from a queue.
Results are returned in the order they are completed.

A simple "run a large batch in parallel" can be defined with a single agent:

.. code-block:: python

    class Thinker(BaseThinker)

        # ...

        @agent
        def run_batch(self):
            # Submit computations
            for x in self.to_run:
                self.queues.send_inputs(x, method='f')

            # Collect results
            results = [self.queues.get_result() for _ in range(len(self.to_run))]

            # Find best
            best_ind = np.argmin([r.value for r in results])
            print(f'Best result: {results[best_in].args}')


We provide a Python API for the message format, :class:`~colmena.models.Result`,
which provides utility operations for tasks that include accessing the positional
or keyword arguments for a task and serializing the inputs and results.

Task Server
+++++++++++

We support Task Servers that `use different workflow engines <task-servers.html>`_,
but all follow the same pattern.
Each are defined by registering computations (often expressed as Python functions) to be run,
a set of available computational resources,
and a queue to communicate with the client.

The best Task Server to start with is Parsl, :class:`~colmena.task_server.parsl.ParslTaskServer`.
Having it run tasks locally can be achieved by

.. code-block::

    # Function
    def f(x):
        return x ** 2 - 3

    # Compute configuration
    from parsl.configs.htex_local import config

    # Communicator
    queues = PipeQueues()

    # Doer
    doer = ParslTaskServer([f], queues, config)


Communication
+++++++++++++

Task requests and results are communicated between Thinker and Doer via queues.
Thinkers submit a task request to one queue and receive results in a second as soon it completes.
Users can also denote tasks with a "topic" to separate tasks used by different agents.

The easiest-to-configure queue, :class:`~colmena.queue.python.PipeQueues`, is based on Python's multiprocessing Pipes.
Creating it requires no other services or configuration beyond the topics:

.. code-block::

    queues = PipeQueues(topics=['steer', 'simulate'])
    queues.send_inputs(1, method='expensive_func', topic='simulation')
    result = queue.get_result(topic='simulation')

Task inputs are serialized using Pickle (we support most Python objects this way),
and task information is communicated over queues as JSON-serialized objects.

`Other implementations <queues.html>`_ of the queue, such as a Redis-backed version (:class:`~colmena.queue.redis.RedisQueue`)
are available.


Life-Cycle of a Task
--------------------

.. TODO (wardlt): Make a figure to illustrate the task routing

We describe the life-cycle of a task to illustrate how all of the components of Colmena work together
by illustrating a typical :class:`~colmena.models.Result` object.

.. code-block:: json
    :linenos:

    {
        "inputs": [[1, 1], {"operator": "add"}],
        "serialization_method": "pickle",
        "method": "reduce",
        "value": 2,
        "success": true,
        "timestamps": {
            "created": 1593498015.132477,
            "input_received": 1593498015.13357,
            "compute_started": 1593498018.856764,
            "result_sent": 1593498018.858268,
            "result_received": 1593498018.860002
        },
        "time": {
            "running": 1.8e-05,
            "serialize_inputs": 4.07e-05,
            "deserialize_inputs": 4.28-05,
            "serialize_results": 3.32e-05,
            "deserialize_results": 3.30e-05
        }
    }

**Launching Tasks**: A client creates a task request at ``timestamp.created`` and adds the the input
specification (``method`` and ``inputs``) to an "outbound" Redis queue. The task request is formatted
in the JSON format defined above with only the ``method``, ``inputs`` and ``timestamp.created`` fields
populated. The task inputs are then serialized (``time.serialize_inputs`` records the execution time)
and passed via the queue to the Task Server.

**Task Routing**: The task server reads the task request from the outbound queue at ``timestamp.input_received``
and submits the task to the distributed workflow engine.
The method definitions in the task server denote on which resources they can run,
and Parsl chooses when and to which resource to submit tasks.

**Computation**: A Parsl worker starts a task at ``timestamp.compute_started``.
The task inputs are deserialized (``time.deserialize_inputs``),
the requested work is executed (``time.running``),
and the results serialized (``time.serialize_results``).

**Result Communication**: The task server adds the result to the task specification (``value``) and
sends it back to the client in an "inbound" queue at (``timestamp.result_sent``).

**Result Retrieval**: The client retrieves the message from the inbound queue.
The result is deserialized (``time_deserialize_result``) and returned
back to the client at ``timestamp.result_received``.

The overall efficiency of the task system can be approximated by comparing the ``time.running``, which
denotes the actual time spent executing the task on the workers, to the difference between the ``timestamp.created``
and ``timestamp.result_returned`` (i.e., the round-trip time).

The overhead specific to Colmena (i.e., and not Parsl) can be measured by assessing the communication time for each step.
For example, the inbound queue can be assessed by comparing the ``timestamp.created`` and ``timestamp.input_received``.
The communication times for Parsl can be measured through the differences between
``timestamp.inputs_received`` and ``timestamp.compute_started``,
provided the task does not wait for a worker to become available.
The communication times related to serialization are also stored (e.g., ``time.serialize_result``).
