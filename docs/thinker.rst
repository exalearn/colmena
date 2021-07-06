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

The ``self.queue`` attribute of a Thinker class manages communication to the task server.
Each agent can use it to submit tasks or wait for results from the task server.

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
            # Process results

