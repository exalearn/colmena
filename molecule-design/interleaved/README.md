# Interleaving Quantum Chem with Reinforcement Learning

An implementation of a design loop where some workers generate new tasks while
other workers execute tasks.

## Application Description

Here, we describe the algorithm implemented by our code. 
See how to run it later. 

### Optimization Algorithm

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

## Configuring the Application

At present, you will need to edit `run.py` and, potentially, a few other files to adapt 
it to a new system.

### Defining Resource Configuration with Parsl

This Colmena app requires two different execution providers: one for the QC calculations
and a second for the ML tasks.

Start by reading the [Colmena documentation for Method Servers](
https://colmena.readthedocs.io/en/latest/how-to.html#specifying-computational-resources)
to understand how Parsl and Colmena can be configured to use place different tasks on
different resources.

The Parsl configuration is defined as `config` when loading the Python modules.
We recommend adding configurations to the [`moldesign/config.py`](../moldesign/config.py)
module to faciliate their re-use between different applications.

Once you select a configuration and configured `run.py` to it use, 
make sure that the methods are mapped to the correct executors
when defining the `ParslMethodServer`.
The two executors should be named `qc` for the Quantum Chemistry methods
and `ml` for the machine learning tasks, but may vary depending on the configuration.

### Altering the Quantum Chemistry Settings

The quantum chemistry code is controlled by three different configuration options.
The beginning of `run.py` defines the specification for the quantum chemistry method
(e.g., whether to use DFT or CCSD(T), basis set choice) and the number of cores per rank
and number of nodes to use for MPI applications (e.g., NWChem).
The `qcengine.yml` file in the same directory as `run.py` also defines the machine-dependent settings
for the MPI application, such as the number of cores per node and the MPI specification.
See the [QCEngine docs](http://docs.qcarchive.molssi.org/projects/QCEngine/en/latest/environment.html#configuration-files)
for more information about the `qcengine.yml` file.

## Running the Application

The `run.py` file is built using `argparse`. Call `python run.py --help` for options.

The code will create an output directory in `runs` that contains the submission time
and a hash of the parameters.
That directory will contain the runtime logs, configuration settings,
and records of the simulations performed.

## Example Configurations

We provide a few configurations as a illustrative examples.

### Debug on Local System

Just running on your local system to test it out.

**Configuration Files**: Use `local_interleaved_config` for the Parsl config,
 which creates two slots on the local machine for quantum chemistry calculations and one for ML tasks.
You should not need to use a `qcengine.yml` file.

**Running the Code**: Call `python run.py` with any desired options. 
Make sure to have launched a `redis-server` first!

**Typical Options**: 
  - `parallel_guesses`: 2, one for each slot
  - `rl_episodes`: 4, to not overburden the machine
  - `search_size`: 10, completes in about 10 min on a desktop
  - `initial_count`: 4, just enough to train a model

### Theta: Split a Single Allocation  

Just running on your local system to keep sane

**Configuration Files**: Use `theta_interleaved_config` for the Parsl config,
 which creates one slot for ML tasks and uses the rest for quantum chemistry.
 NWChem works well with 1-4 nodes for our task sizes, so adjust the number of nodes
 in the `run.py` configuration section and choose the number of nodes in the job
 submission accordingly.

The `qcengine.yml` file for Theta is as follows:

```yaml
all:
  hostname_pattern: "*"
  scratch_directory: ./scratch
  is_batch_node: True
  mpiexec_command: "aprun -n {total_ranks} -N {ranks_per_node} -C -cc depth --env CRAY_OMP_CHECK_AFFINITY=TRUE --env OMP_NUM_THREADS={cores_per_rank} --env MKL_NUM_THREADS={cores_per_rank} -d {cores_per_rank} -j 1"
  jobs_per_node: 1
  ncores: 64
  memory: 192
```

**Running the Code**: Make `scratch` directory. Submit to the batch scheduler with `qsub run.sh` 
with any appropriate settings. The `run.sh` will create the Redis server.

**Typical Options**: 
  - `parallel_guesses`: 1 or two more than the number of simulation slots
  - `rl_episodes`: 10, which completes in a minute or two on a single node 
  - `search_size`: number of slots * 10, so that you get good statistics 
  - `initial_count`: 4, just enough to train a simple model

### Theta + a GPU Cluster: Multi-site with Job Resubmission

This configuration takes a bit work to get running. 
You will need to install Colmena on Theta and another cluster,
and be able to log in to that remote cluster from Theta with SSH.

**Configuration Files**: Use `theta_interleaved_config` for the Parsl config,
which creates two configurations that require further explanation: 

- *`qc` config*: As above, a configuration that runs MPI jobs on Theta.
The key difference is that the configuration will make its own requests for 
allocations on Theta. Consult [Parsl's Theta configuration docs](
https://parsl.readthedocs.io/en/stable/userguide/configuring.html#theta-alcf
) to understand the available options.

- *`ml` config*: This configuration needs to connect out to another cluster and 
launch a Parsl worker that can call back to Theta. For security purposes,
we open an SSH tunnel from the remote system to Theta, which requires fixing
the worker ports and having the external worker connect "to itself" to access
the tunneled ports.
We also define the working directory to be on writable paths on this remote system
and an SSHChannel over which to issue commands.
Finally, we define the worker initialize which requires configuring the correct 
Anaconda environment. 

The code also requires the same `qcengine.yml` file listed for the single-site implementation.

**Running the Code**: The Parsl Method Server must sit on the Theta login node to be able
 to communicate with both sites. Launch it by calling `./run-multisite.sh` to 
 (1) load in the proper conda environment, (2) launch the Redis server, 
 (3) create the SSH tunnels, (4) run Colmena, and (5) kill Redis and the tunnels.
 I highly recommend running the script in a screen so that you can exit the screen
 and kill any daemon processes that do not properly shutdown (there are many
 and they are flaky).
 
**Typical Options**: 
  - `parallel_guesses`: 1 or two more than the number of simulation slots
  - `rl_episodes`: 10, which completes in a minute or two on a single node 
  - `search_size`: number of slots * 10, so that you get good statistics 
  - `initial_count`: 4, just enough to train a simple model
