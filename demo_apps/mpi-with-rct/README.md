# Colmena with Ensembles Involving MPI Tasks

> This example is a work in progress. You can run it with the standard Parsl backend but RCT support is underway

Most HPC using are built using the Message Passing Interface (MPI),
which creates certain challenges in using them in Colmena.
MPI applications are must be invoked using a separate "launcher" application (e.g., `mpiexec`) that 
deploys the executable on remote compute nodes and moving an application between different HPCs often involves
modifying the run script.
The actual functions used to deploy in the HPC application in Colmena are also very complex have significant "boilerplate" tasks such as 
defining these launcher functions, writing the input arguments to input files, and later parsing the results.
This example shows how to avoid some of these issues using the Radical Cybertools Backend to Colmena.

*Note*: The original version Logan wrote does none of those things. It just uses Parsl and a hard-coded MPI launcher.
I'm not actually sure which problems RCT will solve for us, and they could be different to the ones in the intro. 
We adjust the motivation once the features become clearer.

## Installation

We use a fake executable, [`simulation.c`](./simulate.c), as a simulation engine. 
You will need to compile it first: `mpicc simulate.c -o simulate`

Once you install, modify the `mpi_command_string` in `sim.py` as appropriate for your cluster. 

If you are using RCT, set `mpi_command_string` to `None`. RCT will provide the launch command.

## Running

Run the example by calling: `PYTHONPATH="$PYTHONPATH:." python run.py`. 

The run must import an executable description. 

You can change the number of simulations run and the batch size using command line arguments. 
Run `python run.py --help` for further details.
