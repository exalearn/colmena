Pipeline prototyping
====================


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


Separate from the pipeline running, you may pipe a simulated parameter into the pipeline from the
commandline utility ::

  python3 pump.py -p 100


.. note:: The parameter passed to the pump should be an integer.
