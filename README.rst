Pipeline prototyping
====================


Installing
----------


1. Redis: Install Redis on your machine following instructions from `here <https://redis.io/topics/quickstart>`_
2. Python3.6
3. Install the pipeline_prototype::
     cd pipeline_prototype
     pip install .

Running the pipeline
--------------------

To run the pipeline::

  pipeline-main

For more info on arguments for the main pipeline runner::

pipeline-main -h

The default behavior is for the pipeline to kick off a 4 chains of tasks, launch all tasks dispatched
to it via the Redis queues and stop listening on the queue. This allows the pipeline to not block
indefinitely on the redis queue.


Send params
-----------

Once the pipeline is running in blocking mode, you may send as many params as you wish via the `pump.py`
utility. Separate from the pipeline running, you may pipe a simulated parameter into the pipeline from the
commandline utility ::

   pipeline-pump -i -p 100

.. note:: The parameter passed to the pump should be an integer or None.

Stop pipeline
-------------

To stop a pipeline that is running in blocking mode run ::

   pipeline-pump -i -p None

Multi-server implementation prototyping
=======================================

Installing
----------

See above.


Running the system
------------------

To run the system ::

  python3 main.py [-m]

The program waits for requests (integers) submitted via a Redis "input" queue, runs a task for each request, and places the output on a Redis "output" queue. The -m flag allows for running on a Mac.


Send params
-----------

Once the system is running, you may send as many requests as you wish via the `pump.py`
utility, using the -i flag to indicate that these requests are for the input queue. E.g. ::

  python3 pump.py -i -p 100

.. note:: The parameter passed to the pump should be an integer.

Get results
-----------

Once the system is running, you may retrieve results from the output queue via the `pull.py` utility ::

  python3 pull.py

By default, this is a blocking request. To impose a timeout, use the -t parameter, e.g. ::

  python3 pull.py -t 2

Stop pipeline
-------------

To stop a pipeline that is running in blocking mode run ::

  python3 pump.py -p None

