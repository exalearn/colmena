Building Colmena Applications
=============================

Creating a new application with Colmena requires defining the methods to be
deployed on HPC and an application to request those methods.
(See `Design <./design.html>`_ for further details on Colmena architecture).
We describe each of these topics separately.

Configuring a Method Server
---------------------------

The method server for Colmena is configured with the list of methods, a
list available computational resources and a mapping between those two.

Defining Methods
++++++++++++++++

Methods in Colmena are defined as Python functions.
Any Python function can be serviced by Colmena, but
there are several limitations in practice:

1. *Functions must be serializable.* We recommend defining functions in Python
   modules that are accessible from the Python PATH. Consider creating a Python
   module with the functions needed for your application and installing that function
   to the Python path with Pip.
2. *Inputs must be serializable.* Parsl makes a best effort to serialize function
   inputs with JSON, Pickle and other serialization libraries but some object types
   (e.g., thread locks) cannot be serialized.
3. *Functions must be pure.* Colmena is designed with the assumption that the order
   in which you execute tasks does not change the outcomes.

We recommend creating simple Python wrappers for methods which require calling other executables.
The methods will be responsible for generating any required input files and processing the outputs
generated from this method.
Note that we are considering an improved model where the pre- and post-processing methods can
be separate tasks to avoid holding on to large number of nodes
during pre- or post-processing (see `Issue #4 <https://github.com/exalearn/colmena/issues/4>`_).

Specifying Computational Resources
++++++++++++++++++++++++++++++++++

We use `Parsl's resource resource configuration <https://parsl.readthedocs.io/en/stable/userguide/configuring.html>`_
to define available resources for Colmena methods.
We use an complex example that specifies running a mix of single-node and multi-node tasks on
`Theta <https://www.alcf.anl.gov/support-center/theta>`_  to illustrate:

.. code-block:: python

    example_config = Config(
        executors=[
            HighThroughputExecutor(
                address=address_by_hostname(),
                label="multi_node",
                max_workers=8,
                provider=LocalProvider(
                    nodes_per_block=1,
                    init_blocks=1,
                    max_blocks=1,
                    launcher=SimpleLauncher(),  # Places worker on the launch node
                    worker_init='''
    module load miniconda-3
    export PATH=~/software/psi4/bin:$PATH
    conda activate /lus/theta-fs0/projects/CSC249ADCD08/colmena/env
    export NODE_COUNT=4
    ''',
                ),
            ),
            HighThroughputExecutor(
                address=address_by_hostname(),
                label="single_node",
                max_workers=2,
                provider=LocalProvider(
                    nodes_per_block=2,
                    init_blocks=1,
                    max_blocks=1,
                    launcher=AprunLauncher('-d 64 --cc depth'),  # Places worker on compute node
                    worker_init='''
    module load miniconda-3
    export PATH=~/software/psi4/bin:$PATH
    conda activate /lus/theta-fs0/projects/CSC249ADCD08/colmena/env
    ''',
                ),
            ),
            ThreadPoolExecutor(label="local_threads", max_threads=4)
        ],
        strategy=None,
    )

The overall configuration is broken into 3 types of "executors," which each define different
types of resources:

``multi_node``
  The ``multi_node`` executor provides resources for applications that use multiple nodes.
  Note that the executor is deployed using the :class:`parsl.launcher.SimpleLauncher`,
  which means that it will be placed on the same node as the Method Server.
  The maximum number of tasks being run on this resource is defined by ``max_workers``.
  Colmena users are responsible for providing the appropriate ``aprun`` invocation in methods
  deployed on this resource and for controlling the number of nodes used for each task.

``single_node``
  The ``single_node`` executor handles tasks that do not require inter-node communication.
  Parsl places workers on two nodes (see the ``nodes_per_block`` setting) with the ``aprun``
  launcher, as required by Theta. Each node spawns 2 workers and can therefore perform
  two tasks concurrently.

``local_threads``
  The ``local_threads`` is required for Colmena to run output tasks,
  as described in the `design document <design.html#method-server>`_

Mapping Methods to Resources
++++++++++++++++++++++++++++

The constructor of :class:`colmena.method_server.ParslMethodServer` takes a list of
Python function objects as an input.
Internally, the method server converts these to Parsl "apps" by calling
:py:func:`python_app` function from Parsl.
You can pass the keyword arguments for this function along with each function
to map functions to specific resources.

For example, the following code will place requests for the "launch_mpi_application"
method to the "multi_node" resource and the ML task to the "single_node" resource:

.. code-block:: python

    server = ParslMethodServer([
        (launch_mpi_application, {'executor': 'multi_node'}),
        (generate_designs_with_ml, {'executor': 'single_node'})
    ])

Creating a ``main.py``
----------------------

TBD. Discuss a typical, multi-threaded Colmena "Thinker" application


