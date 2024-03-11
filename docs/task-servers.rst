Task Servers Available for Colmena
==================================

Colmena provides multiple `"task servers" <design.html>`_ for executing computations.
Here, we detail the available task servers, describe when they are best used,
and provide the basics of configuring them.

Parsl
-----

:class:`~colmena.task_server.parsl.ParslTaskServer` is the reference implementation for a Colmena task server and is suitable for most use cases.
`Parsl <http://parsl-project.org/>`_ is a distributed workflow engine written in Python that we chose because tasks are described in Python,
workflows can include thousands of concurrent tasks,
and Parsl can be used on many different supercomputing systems.


Configuring Parsl
+++++++++++++++++

Tasks in Parsl are defined using Python functions and are mapped to specific "executors" that control the resources on which they are run.
See `our how-to documentation <how-to.html#definine-methods>`_ for a thorough walkthrough on how to define tasks.
The "executors" describe how many resources to use for each task,
how resources are acquired (e.g., how to inteface with the job scheduler), 
and how each worker communicates with the task server (e.g., address and ports).
The `Parsl documentation <https://parsl.readthedocs.io/en/stable/userguide/configuring.html>`_ explains how to configure executors.

Globus Compute
--------------

The :class:`~colmena.task_server.globus.GlobusComputeTaskServer` uses `Globus Compute <http://funcx.org>`_ to run functions on remote computational resources
in a way that requires less network configuration than with Parsl.
Globus Compute operates by using a cloud-hosted service to facilitate sending function requests to and receiving results from remote
"endpoints" that performs the computation.
In contrast to our Parsl task server, you need not have direct network access (e.g., via SSH) to that system 
or set up SSH tunnels to communicate tasks to or from remote compute nodes.
The ease of multi-site configuration for Globus Compute comes at the cost of higher communication latencies
and limits on the size of inputs or results that are sent over the network.

Configuring Globus Compute
++++++++++++++++++++++++++

Like Parsl, the task server is defined using a list of methods mapped to the resources on which they are executed.
Unlike Parsl, the execution resources are defined using the ID of a Globus Compute endpoint rather than a name of a specific executor.
Any configuration for how that endpoint actually provides compute resources (e.g., launching Kubernetes pods, requesting HPC jobs)
is provided when setting up the endpoint (see `Globus Compute docs <https://funcx.readthedocs.io/en/latest/endpoints.html#example-configurations>`_).

Python's Native executor
------------------------

The :class:`~colmena.task_server.local.LocalTaskServer` is backed by Python's native :class:`~concurrent.futures.Executor` classes.
It is useful for developing new Colmena workflows because it runs with minimal configuraiton.
``LocalTaskServer`` will automatically run workers on as many threads as your computer has processors,
though you can configure it to use separate processes and change the number of workers.
