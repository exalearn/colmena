# Interleaving Quantum Chem with Reinforcement Learning

An implementation of a design loop where some workers generate new tasks while
other workers execute tasks.

*Note*: Make sure to set `OMP_NUM_THREADS=1` when running this program on local resources.
**WARNING**: Only run once on local resources.

## Optimization Algorithm

Our optimization problem is to find a molecule with minimal atomization energy.

We do so by first evaluating `N` random molecules from QM9, using those molecules
to train a simple "group contribution" model with linear regression, and then
find molecules that optimize this model using a Reinforcement Learning search.

At this point, our optimizer launches two threads: a task generator and 
a task consumer.
The task generator continually generates new candidate molecules using the
"train model, run RL, and perform greedy selection" described earlier
The task consume loop reads the most-recent candidates from the task generator, 
manages their completion, and uses them to update the training set
used by the task generator.

## Implementation

The task generator and consumer threads run on the same system.
The task generator write to a priority queue to communicate tasks to 
the consumer, which then writes completed results to a dictionary 
that is shared in memory between the two threads. 

**TBD**: Describe how we configure the distributed compute.


 