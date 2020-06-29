# Greedy Search with Local Retraining

A very simple version of the pipeline.

## Running this Application

First, start a Redis server on your system (`redis-server`).

Then, modify `run.py` to define a Parsl configuration appropriate for your system. 
(Documentation on that is forthcoming). 

Launch the code by calling `python run.py`.
The code is built using `argparse`, so calling `python run.py --help` will describe available options.

The outcome of the run will be stored in a subdirectory of the `runs` folder and named 
using the launch time of the application.

## Optimization Algorithm

Our optimization problem is to find a molecule with minimal atomization energy.

We do so by first evaluating 10 random molecules from QM9, using those molecules
to train a simple "group contribution" model with linear regression, and then
finding molecules that optimize this model using a Reinforcement Learning search.
We then evaluate RL candidate molecules, use that data to update model, 
and re-run the RL selection process with that new model.
This loop repeats until we run out of walltime or perform a set number of evaluations.

## Implementation

Our "Thinker" application performs all of the steps described above sequentially.

Molecules are evaluated and then only after all are complete is the model updated
and new candidates generated.

There are 3 key job types performed on distributed compute resources:
 generating molecules with RL, screening candidates and running quantum chemistry.
Of these, only quantum chemistry is deployed onto compute nodes.
The ML tasks are run on the launch node due to a technical limitation of using QCEngine to 
perform quantum chem applications: the Parsl worker must run on a node with `aprun` (i.e., a MOM node).
We could address this issue by creating a second Parsl provider that deploys tasks onto a compute node, 
but this is out-of-scope for this simple example. 
Each Quantum chemistry application uses a single node (32 MPI ranks with 2 cores each for Theta).  
