Types of Colmena Methods
========================

Colmena encapsulates the methods being executed with a wrapper that
manages deserializing task data and tracking runtime information.

The `task servers <task-servers.html>`_ automatically determine
the correct wrapper for Python tasks, though you will 
need to instantiate your own wrapper `for non-Python tasks <#running-executable>`_.

While you thus do not need to know about the wrappers in most cases,
we describe what each does and how they can be adjusted here.

.. note::

    Apply the ``PythonGeneratorMethod`` wrapper before supplying to
    the Task Server for wrapped generator functions.
    Automatic detection of `Python generator function <#generator-python-functions>`_
    usually fails for wrapped functions.


Basic Python Functions
----------------------

The "basic" Python function has a single return value.

.. code-block:: python

    def f(x: int) -> int:
        return x * 2

The wrapper for this type of function, :class:`~colmena.models.methods.PythonMethod` needs only a reference to the function
and a name to know the function by, if different than the function's existing name.

.. code-block:: python

    from colmena.models.tasks import PythonMethod

    wrapper = PythonMethod(
        function=f,
        name='new_name'
    )

Generator Python Functions
--------------------------

Python generator functions produce a continual series of outputs then, in some cases, a value on completion.

.. code-block:: python

    def g(x: int) -> Iterable[int]:
        yield from range(x)
        return "done"

Like the single-return wrapper, Colmena wraps these functions using a reference to the function and a name.
There is an additional option about whether to return the "return" value in addition to the "yield" values.
Colmena returns only the yielded values by default.

.. code-block:: python

    from colmena.models.methods import PythonGeneratorMethod

    wrapper = PythonGeneratorMethod(
        function=f,
        store_return_value=True
    )


Setting ``store_return_values`` to ``True`` will return a tuple of results, such as ``([1], "done")`` for ``x=1``, 
and return only the first element of the tuple if ``False``.

Streaming Results
+++++++++++++++++

Configure a generator tasks to stream results as soon as they are created by
supplying a :class:`~colmena.queue.base.ColmenaQueues` when defining the method.


.. code-block:: python

    queues = RedisQueues()
    wrapper = PythonGeneratorMethod(
        function=f,
        store_return_value=True
        streaming_queue=queues
    )


The Thinker will receive the yielded results over the task queue provied to the function.
Each of the yielded result will have the ``completed`` field of the :class:`~colmena.models.results.Results` 
set to ``False``, wheras the returned value will have a value of ``True``.

.. note::

    We recommend using :class:`~colmena.queue.redis.RedisQueues` with Redis configured to accept 
    connections from other nodes if workers are run on a different node than the Thinker.

Running Executables
-------------------

All tasks in Colmena require a Python interface to be executed in the workflow
and the :class:`~colmena.models.methods.ExecutableMethod` as a guiderail for including
computations that are performed outside of Python.

The definition of an ``ExectuableMethod`` is split into three parts:

1. ``__init__``: create the shell command needed to launch your code and pass it to the initializer of the base class.
2. ``preprocess``: use method arguments to create the input files, command line arguments, or stdin needed to execute
   the simulation code with the desired settings
3. ``postprocess``: extract the desired outputs for the function from any files or the standard out produced
   when executing the code.

The example code below runs the ``simulator`` software, which reads inputs from CLI arguments and from a ``options.json`` file
then stores the result in stdout.

.. code-block:: python

    class Simulation(ExecutableMethod):

        def __init__(self):
            super().__init__(executable=['/path/to/my/simulator'], name='simulator')

        def preprocess(self, run_dir, args, kwargs):
            with open(run_dir / 'option.json', 'w') as fp:
                json.dump(kwargs, fp)  # Write any kwargs to disk
            return [str(args[0])], None  # Uses the args as CLI arguments

        def postprocess(self, run_dir: Path):
            # The stdout of the code is routed to `colmena.stdout`
            with open(run_dir / 'colmena.stdout') as fp:
                return float(fp.read().strip())

Some Task Server implements execute the pre- and post-processing step on separate resources
from the executable task to make more efficient use of the compute resources.

See the `MPI example <https://github.com/exalearn/colmena/tree/master/demo_apps/mpi-with-rct>`_.

MPI Applications
++++++++++++++++

Message-Passing Interface (MPI) codes are the standard type of application that
utilize multiple nodes of a supercomputer for the same task.
In addition to defining the path to the executable and processing operations, MPI codes
also require a definition of how to launch the executable across many compute nodes.

For most cases, provide these option in the ``__init__`` method of your executable and set the ``mpi`` option to ``True``.

.. code-block:: python

    class Simulation(ExecutableMethod):

        def __init__(self):
            super().__init__(
                executable=['/path/to/my/simulator'],
                name='simulator',
                mpi=True,  # Designate this as an MPI application
                mpi_command_string='mpirun -np {total_ranks}',  # Optionally provide the MPI invocation template
            )

Some workflow tools, like RCT, can supply the ``mpi_command_string`` information automatically.

Specify the number of nodes and ranks per node for each tasks using the ``resources`` keyword argument
during task submission.

.. code-block:: python

    client_queue.send_inputs(1,  method='simulator', resources={'node_count': 2})
