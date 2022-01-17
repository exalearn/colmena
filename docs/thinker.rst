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
For example, the ``startup`` keyword argument means that the ``self.done`` event will not
be set when this agent completes.

See :func:`colmena.thinker.agent` for more details.

Special-Purpose Agents
----------------------

There are a few common patterns of agents within Colmena,
such as agents that wait for results to become available in a queue.
We provide decorators that simplify creating agents for such tasks.

The `reallocation example application <https://github.com/exalearn/colmena/tree/master/demo_apps/reallocation-example>`_
demonstrates all three of these agent types.

Result Processing Agents
++++++++++++++++++++++++

The :func:`colmena.thinker.result_processor` is for agents that respond to results becoming available.
It takes a single argument that defines which topic queue to be associated with and
must decorate a function that takes Result object as an input.

.. code-block:: python

    class Thinker(BaseThinker):
        @result_processor(topic='simulation')
        def process(self, result: Result):
            self.database.append(result)

The above example runs the ``process`` function whenever a complete task with a "simulation" topic is received.

Task Submission Agents
++++++++++++++++++++++

Task submission agents execute a function as soon as resources are available.
The agent runs a decorated function once resources are acquired from a certain resource pool.
Task submission agents are often paired with a `result processor <#result-processing-agents>`_ that
receives the result and marks resources as available once a task completes.

.. code-block:: python

    class Thinker(BaseThinker):
        @task_submitter(task_type="sim", n_slots=1)
        def submit(self):
            task = self.queue.pop(0)
            self.queues.send_inputs(task, method='simulate', topic='simulation')

The above function submits a task from the front of a task queue once 1 slot is
available from the "sim" resource pool.

Event Responder Agents
++++++++++++++++++++++

The :func:`colmena.thinker.event_responder` runs a function when a certain event is triggered.
The event responder agents can be configured to request resources in a background thread that are
then deallocated after the function completes.

.. code-block:: python

    class Thinker(BaseThinker):
        @event_responder(event_name='retrain_now', reallocate_resources=True,
                         gather_from="sim", gather_to="ml", disperse_to="sim", max_slots=1)
        def reorder(self):
            self.retrain_now.clear()  # Clear the event flag

            # Submit a task to re-order task queue given
            self.rec.allocate('ml', 1)  # Blocks until resources are free
            self.queues.send_inputs(self.database, self.queue, method='reorder', topic='plan')

            # Wait for task to complete
            result = self.queues.get_result(topic='plan')
            self.rec.release('ml', 1)  # Mark that resources are unneeded

            # Store the new task queue
            self.queue = result.value


The above example performs a task to reorder the task queue when the ``retrain_now`` event is set.
Colmena will automatically re-allocate resources from simulation to machine learning when the event
is set and then re-allocate them back to simulation after the function completes.
