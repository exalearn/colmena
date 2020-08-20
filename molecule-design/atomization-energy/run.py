import pickle as pkl
import argparse
import hashlib
import json
import sys
import logging
import os
from random import sample, choice, shuffle, random
from datetime import datetime
from functools import partial, update_wrapper
from queue import Queue, Empty
from threading import Thread, Event
from typing import List, Dict

import parsl
import numpy as np
import tensorflow as tf
from qcelemental.models.procedures import QCInputSpecification
from molgym.agents.moldqn import DQNFinalState
from molgym.envs.rewards.mpnn import MPNNReward
from molgym.mpnn.layers import custom_objects

from moldesign.score.mpnn import evaluate_mpnn, update_mpnn, MPNNMessage
from moldesign.config import theta_xtb_config
from moldesign.sample.moldqn import generate_molecules
from moldesign.simulate import compute_atomization_energy
from moldesign.utils import get_platform_info
from colmena.method_server import ParslMethodServer
from colmena.redis.queue import ClientQueues, make_queue_pairs
from colmena.models import Result

# Define the compute setting for the system (only relevant for NWChem)
compute_config = {'nnodes': 1, 'cores_per_rank': 2}


class Thinker(Thread):
    """ML-enhanced optimization loop for molecular design"""

    def __init__(self, queues: ClientQueues,
                 initial_training_set: Dict[str, float],
                 initial_search_space: List[str],
                 initial_moldqn: DQNFinalState,
                 initial_mpnns: List[tf.keras.Model],
                 output_dir: str,
                 n_parallel: int = 1,
                 n_molecules: int = 10,
                 queue_length: int = None,
                 random_frac: float = 0.1,
                 greedy_frac: float = 0.8):
        """
        Args:
            queues (ClientQueues): Queues to use to communicate with server
            initial_training_set: List of molecules and atomization energies from the original search
            initial_search_space: List of molecules to use in the initial search space
            initial_moldqn: Pre-trained version of the MolDQN agent
            initial_mpnn: Pre-trained version of the MolDQN agent
            output_dir (str): Path to the run directory
            n_parallel (int): Maximum number of QC calculations to perform in parallel
            n_molecules: Number of molecules to evaluate
            queue_length (int): Number of tasks to store in the queue at most
            random_frac: Number of molecules to pick at random
            greedy_frac: Number of molecules to pick greedly
        """
        super().__init__(daemon=True)

        # Generic stuff: logging, communication to Method Server
        self.queues = queues
        self.logger = logging.getLogger(self.__class__.__name__)
        self.output_dir = output_dir

        # The ML components
        self.moldqn = initial_moldqn
        self.mpnns = initial_mpnns
        
        # Active learning settings
        self.random_frac = random_frac
        self.greedy_frac = greedy_frac

        # Attributes associated with quantum chemistry calculations
        # TODO (wardlt): Use QCFractal or another database system instead of volatile in-memory databases
        self.database = initial_training_set.copy()
        self.search_space = initial_search_space

        # Attributes associated with the active learning
        self.n_evals = n_molecules + len(self.database)
        self.n_parallel = n_parallel

        # Synchronization between ML and QC loops
        if queue_length is None:
            queue_length = n_parallel * 2
        self.queue_length = queue_length
        self._task_queue = Queue(maxsize=queue_length)
        self._gen_done = Event()

    def _write_result(self, result: Result, filename: str, keep_inputs: bool = True, keep_outputs: bool = True):
        """Write result to a log file

        Args:
            result: Result to be written
            filename: Name of the log file
            keep_inputs: Whether to write the function inputs
            keep_outputs: Whether to write the function outputs
        """

        # Determine which fields to dumb
        exclude = set()
        if not keep_inputs:
            exclude.add('inputs')
        if not keep_outputs:
            exclude.add('value')

        # Write it out
        with open(os.path.join(self.output_dir, filename), 'a') as fp:
            print(result.json(exclude=exclude), file=fp)

    def simulation_dispatcher(self):
        """Runs the ML loop: Generate tasks for the simulator"""

        self.logger.info('Simulation dispatcher waiting for work')
        for i in range(self.n_parallel):
            smiles, task_info = self._task_queue.get(block=True)
            if smiles in self.search_space:
                self.search_space.remove(smiles)
            self.queues.send_inputs(smiles, topic='simulator', method='compute_atomization_energy', keep_inputs=True,
                                    task_info=task_info)
        self.logger.info('Sent out first set of tasks')

        # As they come back submit new ones
        while not self._gen_done.is_set():
            # Get the task and store its content
            result = self.queues.get_result(topic='simulator')
            self.logger.info('QC task completed')
            if result.success:
                # Store the result in the database
                self.database[result.args[0]] = result.value[0]  # First arg is the energy
                
                # Save the data
                self._write_result(result.value[1], 'qcfractal_records.jsonld')
                if result.value[2] is not None:
                    self._write_result(result.value[2], 'qcfractal_records.jsonld')
                result.value = result.value[0]  # Do not store the full results in the database
            else:
                logging.warning('Calculation failed! See simulation outputs and Parsl log file')
            self._write_result(result, 'simulation_records.jsonld', keep_outputs=True)

            # Get a new one from the priority queue and submit it
            smiles, task_info = self._task_queue.get()
            if smiles in self.search_space:
                self.search_space.remove(smiles)
            self.logger.info(f'Running {smiles} from batch {task_info["batch"]}')
            self.queues.send_inputs(smiles, topic='simulator', method='compute_atomization_energy', keep_inputs=True,
                                    task_info=task_info)

        # Waiting for the still-ongoing tasks to complete
        self.logger.info('Collecting the last molecules')
        for i in range(self.n_parallel):
            # Get the task and store its content
            result = self.queues.get_result(topic='simulator')
            self.logger.info(f'Retrieved {i+1}/{self.n_parallel} on-going tasks')
            if result.success:
                # Store the result in the database
                self.database[result.args[0]] = result.value[0]  # First arg is the energy

                self._write_result(result.value[1], 'qcfractal_records.jsonld')
                if result.value[2] is not None:
                    self._write_result(result.value[2], 'qcfractal_records.jsonld')
                result.value = result.value[0]  # Do not store the full results in the database
            else:
                logging.warning('Calculation failed! See simulation outputs and Parsl log file')
            self._write_result(result, 'simulation_records.jsonld', keep_outputs=True)

        self.logger.info('Task consumer has completed')

    def run(self):
        # Launch the "simulator" thread
        design_thread = Thread(target=self.simulation_dispatcher)
        design_thread.start()

        # Submit some initial molecules so that the simulator gets started immediately
        num_to_seed = self.queue_length
        self.logger.info(f'Sending {num_to_seed} initial molecules')
        for smiles in sample(self.search_space, num_to_seed):
            # We send: (rank info), smiles, task_info
            self._task_queue.put((smiles, {'reason': 'initial', 'batch': 0, 'smiles': smiles}))

        # Perform the design loop iteratively
        step_number = 0
        self.logger.info(f'Running until database has {self.n_evals} entries')
        while len(self.database) < self.n_evals:
            self.logger.info(f'Generating new molecules')

            # Update the MPNNs
            for i, mpnn in enumerate(self.mpnns):
                self.queues.send_inputs(MPNNMessage(mpnn), self.database, 4,
                                        method='update_mpnn', topic='ML', task_info={'index': i})
            self.logger.info(f'Updating the model with training set size {len(self.database)}')
            
            for _ in range(len(self.mpnns)):
                result = self.queues.get_result(topic='ML')
                new_weights, _ = result.value
                self.mpnns[result.task_info['index']].set_weights(new_weights)
                self._write_result(result, 'update_records.jsonld', keep_inputs=False, keep_outputs=False)

            # Use RL to generate new molecules
            self.moldqn.env.reward_fn.model = choice(self.mpnns)
            self.queues.send_inputs(self.moldqn, method='generate_molecules', topic='ML')
            result = self.queues.get_result(topic='ML')
            self._write_result(result, 'generate_records.jsonld', keep_inputs=False, keep_outputs=False)
            new_molecules, self.moldqn = result.value  # Also update the RL agent
            self.logger.info(f'Generated {len(new_molecules)} candidate molecules')

            # Update the list of molecules
            self.search_space = list(set(self.search_space).union(new_molecules))
            self.logger.info(f'Search space now includes {len(self.search_space)} molecules')

            # Assign them scores
            self.queues.send_inputs([MPNNMessage(m) for m in self.mpnns],
                                    new_molecules, method='evaluate_mpnn', topic='ML')
            result = self.queues.get_result(topic='ML')
            scores = result.value
            self._write_result(result, 'screen_records.jsonld', keep_inputs=False, keep_outputs=False)
            self.logger.info(f'Assigned scores to all molecules')

            # Assign scores to each SMILES
            mean_score = scores.mean(axis=1)
            std_score = scores.std(axis=1)
            task_options = [{'smiles': s, 'pred': float(m), 'pred_std': float(u), 'batch': step_number} 
                            for s, m, u in zip(self.search_space, mean_score, std_score)]
            
            # Rank according to different metrics. Best at the right end (so .pop works)
            random_selections = task_options.copy()
            shuffle(random_selections)
            greedy_selections = sorted(task_options, key=lambda x: -x['pred'])
            uq_selections = sorted(task_options, key=lambda x: x['pred_std'])
            self.logger.info('Sorted molecules by greedy, random and uncertainty selection')
            
            # Pick enough to fill the queue
            already_picked = set()
            selections = []
            while len(already_picked) < self.queue_length:
                # Pick a task
                r = random()
                if r < self.greedy_frac:
                    task = greedy_selections.pop()
                    task['reason'] = 'greedy'
                elif r < self.greedy_frac + self.random_frac:
                    task = random_selections.pop()
                    task['reason'] = 'random'
                else:
                    task = uq_selections.pop()
                    task['reason'] = 'uq'
                    
                # If it is not yet selected
                if task['smiles'] not in already_picked:
                    already_picked.add(task['smiles'])
                    selections.append(task)
            self.logger.info(f'Selected {len(selections)} new molecules')
            
            # Clear out the queue
            while not self._task_queue.empty():
                try:
                    self._task_queue.get_nowait()
                except Empty:
                    break
            self.logger.info('Cleared out the current queue')

            # Add requested simulations to the queue
            for rank, task in enumerate(selections):
                self._task_queue.put((task['smiles'], task))
            step_number += 1  # Increment the loop
            self.logger.info('Added all of them the task queue')

        self.logger.info('No longer generating new candidates')
        self._gen_done.set()


if __name__ == '__main__':
    # User inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--redishost", default="127.0.0.1",
                        help="Address at which the redis server can be reached")
    parser.add_argument("--redisport", default="6379",
                        help="Port on which redis is available")
    parser.add_argument('--mpnn-config-directory', help='Directory containing the MPNN-related JSON files',
                        required=True)
    parser.add_argument('--mpnn-model-files', nargs="+", help='Path to the MPNN h5 files', required=True)
    parser.add_argument('--initial-agent', help='Path to the pickle file for the MolDQN agent', required=True)
    parser.add_argument('--initial-search-space', help='Path to an initial population of molecules', required=True)
    parser.add_argument('--initial-database', help='Path to the database used to train the MPNN', required=True)
    parser.add_argument('--reference-energies', help='Path to the reference energies for the QC calculations',
                        required=True)
    parser.add_argument('--qc-spec', help='Path to the QC specification', required=True)
    parser.add_argument("--parallel-guesses", default=1, type=int,
                        help="Number of calculations to maintain in parallel")
    parser.add_argument("--rl-episodes", default=10, type=int,
                        help="Number of episodes to run ing the reinforcement learning pipeline")
    parser.add_argument("--search-size", default=1000, type=int,
                        help="Number of new molecules to evaluate during this search")
    parser.add_argument('--queue-length', default=100, type=int, help="Number of molecules to hold in queue")
    parser.add_argument('--random-frac', default=0.1, type=float, help="Number of new molecules to pick randomly")
    parser.add_argument('--greedy-frac', default=0.8, type=float, help="Number of new molecules to pick greedly")

    # Parse the arguments
    args = parser.parse_args()
    run_params = args.__dict__
    
    # Load in the models, initial dataset, agent and search space
    mpnns = [
        tf.keras.models.load_model(path, custom_objects=custom_objects)
        for path in args.mpnn_model_files
    ]                                 
    with open(os.path.join(args.mpnn_config_directory, 'atom_types.json')) as fp:
        atom_types = json.load(fp)
    with open(os.path.join(args.mpnn_config_directory, 'bond_types.json')) as fp:
        bond_types = json.load(fp)
    with open(args.initial_database) as fp:
        initial_database = json.load(fp)
    with open(args.reference_energies) as fp:
        ref_energies = json.load(fp)
    with open(args.initial_search_space) as fp:
        initial_search_space = json.load(fp)
    with open(args.initial_agent, 'rb') as fp:
        agent = pkl.load(fp)
    with open(args.qc_spec) as fp:
        qc_spec = json.load(fp)
    code = qc_spec.pop("program")
    qc_spec = QCInputSpecification(**qc_spec)

    # Make the reward function
    agent.env.reward_fn = MPNNReward(mpnns[0], atom_types, bond_types, maximize=False)

    # Create an output directory with the time and run parameters
    start_time = datetime.utcnow()
    params_hash = hashlib.sha256(json.dumps(run_params).encode()).hexdigest()[:6]
    out_dir = os.path.join('runs', f'{start_time.strftime("%d%b%y-%H%M%S")}-{params_hash}')
    os.makedirs(out_dir, exist_ok=True)

    # Save the run parameters to disk
    run_params['version'] = 'simple'
    with open(os.path.join(out_dir, 'run_params.json'), 'w') as fp:
        json.dump(run_params, fp, indent=2)
    with open(os.path.join(out_dir, 'qc_spec.json'), 'w') as fp:
        print(qc_spec.json(), file=fp)

    # Save the platform information to disk
    host_info = get_platform_info()
    with open(os.path.join(out_dir, 'host_info.json'), 'w') as fp:
        json.dump(host_info, fp, indent=2)

    # Set up the logging
    handlers = [logging.FileHandler(os.path.join(out_dir, 'runtime.log')),
                logging.StreamHandler(sys.stdout)]

    class ParslFilter(logging.Filter):
        """Filter out Parsl debug logs"""

        def filter(self, record):
            return not (record.levelno == logging.DEBUG and '/parsl/' in record.pathname)

    for h in handlers:
        h.addFilter(ParslFilter())

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO, handlers=handlers)

    # Write the configuration
    config = theta_xtb_config(1, os.path.join(out_dir, 'run-info'), xtb_per_node=2)
    parsl.load(config)

    # Save Parsl configuration
    with open(os.path.join(out_dir, 'parsl_config.txt'), 'w') as fp:
        print(str(config), file=fp)

    # Connect to the redis server
    client_queues, server_queues = make_queue_pairs(args.redishost, args.redisport,
                                                    serialization_method="pickle",
                                                    topics=['simulator', 'ML'], keep_inputs=False)

    # Apply wrappers to functions to affix static settings
    #  Update wrapper changes the __name__ field, which is used by the Method Server
    #  TODO (wardlt): Have users set the method name explicitly
    my_generate_molecules = partial(generate_molecules, episodes=args.rl_episodes)
    my_generate_molecules = update_wrapper(my_generate_molecules, generate_molecules)

    my_compute_atomization = partial(compute_atomization_energy, compute_hessian=False,
                                     qc_config=qc_spec, reference_energies=ref_energies,
                                     compute_config=compute_config, code=code)
    my_compute_atomization = update_wrapper(my_compute_atomization, compute_atomization_energy)

    my_evaluate_mpnn = partial(evaluate_mpnn, atom_types=atom_types, bond_types=bond_types)
    my_evaluate_mpnn = update_wrapper(my_evaluate_mpnn, evaluate_mpnn)

    my_update_mpnn = partial(update_mpnn, atom_types=atom_types, bond_types=bond_types)
    my_update_mpnn = update_wrapper(my_update_mpnn, update_mpnn)

    # Create the method server and task generator
    ml_cfg = {'executors': ['ml']}
    dft_cfg = {'executors': ['qc']}
    doer = ParslMethodServer([(my_generate_molecules, ml_cfg), (my_evaluate_mpnn, ml_cfg),
                              (my_update_mpnn, ml_cfg), (my_compute_atomization, dft_cfg)],
                             server_queues)

    # Configure the "thinker" application
    thinker = Thinker(client_queues,
                      initial_database,
                      initial_search_space,
                      agent,
                      mpnns,
                      output_dir=out_dir,
                      n_parallel=args.parallel_guesses,
                      n_molecules=args.search_size,
                      queue_length=args.queue_length,
                      random_frac=args.random_frac,
                      greedy_frac=args.greedy_frac)
    logging.info('Created the method server and task generator')

    try:
        # Launch the servers
        #  The method server is a Thread, so that it can access the Parsl DFK
        #  The task generator is a Thread, so that all debugging methods get cast to screen
        doer.start()
        thinker.start()
        logging.info('Launched the servers')

        # Wait for the task generator to complete
        thinker.join()
        logging.info('Task generator has completed')
    finally:
        client_queues.send_kill_signal()

    # Wait for the method server to complete
    doer.join()
