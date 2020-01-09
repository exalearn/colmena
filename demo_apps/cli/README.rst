Running the pipeline
--------------------

This demonstration app uses three command-line applications to illustrate how the ``MethodServer``
can be used from separate applications.

You will need to install the applications first by calling ``pip install -e .``

Launching the ``MethodServer``
=============================

To run the pipeline::

  pipeline-main

For more info on arguments for the main pipeline runner::

  pipeline-main -h

The default behavior is for the pipeline to kick off a 4 chains of tasks, launch all tasks dispatched
to it via the Redis queues and stop listening on the queue. This allows the pipeline to not block
indefinitely on the redis queue.

Send params
===========

Once the pipeline is running in blocking mode, you may send as many params as you wish via the `pump.py`
utility. Separate from the pipeline running, you may pipe a simulated parameter into the pipeline from
the commandline utility ::

   pipeline-pump -i -p 5

.. note:: The parameter passed to the pump should be an integer or None.

Get results
===========

Once the system is running, you may retrieve results from the output queue via the `pull.py` utility ::

  pipeline-pull

By default, this is a blocking request. To impose a timeout, use the -t parameter, e.g. ::

  pipeline-pull -t 2


Stop pipeline
=============

To stop a pipeline that is running in blocking mode run ::

  pipeline-pump -p None
