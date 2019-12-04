1) Pipeline prototyping
=======================

This is Yadu's original pipeline prototype, developed to show how to tasks from a Redis queue.

Installing
----------


1. Redis: Install Redis on your machine following instructions from `here <https://redis.io/topics/quickstart>`_
2. Python3.6
3. Install python modules::

     pip install -r requirements.txt



Running the pipeline
--------------------

To run the pipeline::

  python3 pipeline.py

The default behavior is for the pipeline to kick off a 4 chains of tasks, launch all tasks dispatched
to it via the Redis queues and stop listening on the queue. This allows the pipeline to not block
indefinitely on the redis queue.

To block on the redis_q use the `-b` flag to the pipeline::

  python3 pipeline.py -b

Send params
-----------

Once the pipeline is running in blocking mode, you may send as many params as you wish via the `pump.py`
utility. Separate from the pipeline running, you may pipe a simulated parameter into the pipeline from the
commandline utility ::

  python3 pump.py -p 100

.. note:: The parameter passed to the pump should be an integer.

Stop pipeline
-------------

To stop a pipeline that is running in blocking mode run ::

  python3 pump.py -p None
  
 
2) Multi-server implementation prototyping
==========================================

This version extends the first version to output results to another Redis queue. 

Overview
--------

An instance of the `MpiMethodServer` class (mpi_method_server.py) is created with input and output queues.
The `main_loop` method implements the persistent method server. It repeatedly:

* receives a request on the input queue
* requests creation of an "MPI computation" (currently a dummy operation) to process the request 
* sends the result of the computation on the output queue

Notes:

* The output queue might be piped directly to a value server. (Or, alternatively, the value server functionality could be integrated with that of the method server?)

* The implementation does not handle failures

* The implementation does not throttle the number of MPI computation requests made


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

