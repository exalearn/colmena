Advanced Features for Thinkers
==============================

The core of a Colmena application is a "thinker" process that controls the use of
computational resources by submitting new tasks in response to new data being acquired.
This portion of the guide delves into the advanced features for building a "Thinker" application
and builds the steps described in `the previous page of the guide <./how-to.html#creating-a-thinker-application>`_.

Objects Shared by Default
-------------------------

Several objects are available to all "agent" threads available in a Thinker application.

Queues
++++++

The ``self.queue`` attribute of a Thinker class manages communication to the method server.
Each agent can use it to submit tasks or wait for results from the method server.

The :class:`colmena.redis.queue.ClientQueues` object must be provided to the constructor.

Logger
++++++

The ``self.logger`` attribute is a logger unique to each thread.
Log messages written with this attribute will be marked with the name of the agent.

Completion Flag
+++++++++++++++

Upon completion, an agent sets the ``self.done`` flag.
All threads may view the status of this event.

Resource Counter
++++++++++++++++

The ``self.rec`` attribute is used to communicate the availability of compute resources between threads.
Threads may release resources to make them available for use by other agents, request resources, or
transfer resources between different available pools.

A ``ResourceCounter`` that is configured with the proper number of slots and task pools must be provided
to the constructor for this feature to be available.

See the documentation for :class:`colmena.thinker.resources.ResourceCounter`.

Configuring General Agents
--------------------------

Agent threads in Colmena take a few different configuration options.
For example, the ``critical`` keyword argument means that the ``self.done`` attribute will not
be flagged when this thread executes.

See :func:`colmena.thinker.agent` for more details.

Special-Purpose Agents
----------------------

There are a few common patterns of agents within Colmena,
such as agents that wait for results to become available in a queue.
We provide decorators that simplify creating agents for such tasks.

Result Processing Agents
++++++++++++++++++++++++

The :func:`colmena.thinker.result_processor` is for agents that respond to results becoming available.
It takes a single argument that defines which topic queue to be associated with and
must decorate a function that takes Result object as an input.

.. code-block:: python

    class Thinker(BaseThinker):
        @result_processor(topic='simulation')
        def process(self, result: Result):
            # Do some compute that that result
            self.database.append((result.args, results.value))

Task Submission Agents
++++++++++++++++++++++

The task submission agents react to the availability of computational resources.
The :func:`colmena.thinker.task_submitter` decorator tasks the pool of resources
to draw from and the number of slots needed for this agent to begin processing.
Agent functions have no arguments.

.. code-block:: python

    class Thinker(BaseThinker)
        @task_submitter(n_slots=4, task_type="simulation")
        def submit_new_simulation(self):
            self.queues.submit_task(self.task_queue.pop(), method='simulate')


Event Responder Agent
+++++++++++++++++++++

The :func:`colmena.thinker.event_responder` waits for an event associated with an thinker being set.
The ``event_name`` is the name of a class attribute of the thinker class.

.. code-block:: python

    class Thinker(BaseThinker):

        def __init__(self, queues):
            super().__init__(queues)
            self.flag = Event()

        @event_responder(event_name="flag")
        def responder(self):
            self.flag.clear()  # Mark that we saw the event
            # Do something about it
