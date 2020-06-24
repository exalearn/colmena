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
The priority queue is sorted first so that the most-recent entries
are pulled first and, within the most-recent batch,
the entries that are ranked the highest.
In this way, the method server chooses the highest-ranked
entry from the most-recent batch. 

The task consumer and generator both request three kinds of tasks:
1. Compute atomization energy an initial population of molecules
2. Generate new molecular candidates with RL
3. Screen candidate molecules with a supervised learning model

We partition our available nodes such that all but one node is available to run
the quantum chemistry (Task 1) and the ML tasks (Tasks 2-3).
We accomplish this by creating two different "executors" with Parsl for the quantum 
chem and ML tasks.
We configure the method server in Colmena to only allow the functions from each 
type of task to use the correct partition (see the `ml_cfg` and `dft_cfg` lines and
how they are used to create the method server). 

We use the "topics" feature of the Colmena queues to ensure that the consumer 
and generator threads receive the correct tasks.
When submitting tasks, each thread labels their task with a topic ("ML" for the generator,
and "simulator" for the consumer). 
When required, each thread waits for a result with the designated topic to be returned. 
In this way, both the creator and generator threads can communicate with the same
method server and the programmer need not write custom logic to associate task result
with the proper thread.
