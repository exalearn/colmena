# Molecular Optimization for Atomization

This application is designed to find molecules with minimal atomization energies
using a combination of DFT, message-passing neural networks and reinforcement-learning-based
molecular generation schemes. 

## Approach

We use a two-level optimization scheme.

Level 1 is a Reinforcement Learning algorithm that attempts to find molecules with 
minimal atomization energy estimated by a MPNN model.
The MPNN operates using only the graph structure for the molecule,
which allows it to make predictions very quickly and simplifies the
RL agent by only considering molecular bonding and not 3D conformer structure.

Level 2 is an Active Learning optimization where we find the molecules
suggested by Level 1 which have the lowest atomization energies.
Here, we use quantum chemistry to compute the atomization energy 
rather than relying on an MPNN to estimate it.
The active learning algorithm uses predictions from the MPNNs used
in Level 1 to identify which quantum chemistry calculations are the most valuable.
The new quantum chemistry calculations are used to re-train the MPNN, 
which is sent back to the "Level 1" optimizer.

## Running this Application

First, start a Redis server on your system (`redis-server`).

Then, modify `run.py` to define a Parsl configuration appropriate for your system. 
(Documentation on that is forthcoming). 

Launch the code by calling `python run.py`.
The code is built using `argparse`, so calling `python run.py --help` will describe available options.

The outcome of the run will be stored in a subdirectory of the `runs` folder and named 
using the launch time of the application.

## Optimization Algorithm

TBD. Will fill in after I get this running on Theta

## Implementation

TBD. Will fill in after I get this running on Theta
