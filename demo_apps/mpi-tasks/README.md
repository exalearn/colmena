# Colmena with Ensembles Involving MPI Tasks

Most HPC applications use the Message Passing Interface (MPI),
which creates certain challenges in using them in Colmena.
MPI applications must be invoked using a separate "launcher" application (e.g., `mpiexec`) that 
deploys the executable on remote compute nodes and moving an application between different HPCs often involves
modifying the run script.
The actual functions used to deploy in the HPC application in Colmena are also very complex have significant "boilerplate" tasks such as 
defining these launcher functions, writing the input arguments to input files, and later parsing the results.
This demo shows an approach for deploy MPI applications in Colmena,
as described in [the documentation](https://colmena.readthedocs.io/en/latest/how-to.html#methods-that-used-compiled-applications).

## Installation

We use a fake executable, [`simulation.c`](./simulate.c), as a simulation engine. 
You will need to compile it first: `mpicc simulate.c -o simulate`

Once you install, modify the `mpi_command_string` in `sim.py` as appropriate for your cluster.

## Running

Run the example by calling: `PYTHONPATH="$PYTHONPATH:." python run.py`. 

The run must import an executable description. 

You can change the number of simulations run and the batch size using command line arguments. 
Run `python run.py --help` for further details.
