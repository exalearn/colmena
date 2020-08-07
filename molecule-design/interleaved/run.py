import argparse
import hashlib
import json
import sys
import logging
import os
from datetime import datetime
from functools import partial, update_wrapper
from queue import PriorityQueue
from threading import Thread, Event
from typing import List

import parsl
import numpy as np
from molgym.agents.moldqn import DQNFinalState
from molgym.agents.preprocessing import MorganFingerprints
from molgym.envs.simple import Molecule
from qcelemental.models.procedures import QCInputSpecification, Model
from sklearn.linear_model import BayesianRidge
from sklearn.pipeline import Pipeline

from moldesign.config import theta_interleaved_config as config
from moldesign.sample.moldqn import generate_molecules, SklearnReward
from moldesign.score import compute_score
from moldesign.score.group_contrib import GroupFeaturizer
from moldesign.select import greedy_selection
from moldesign.simulate import compute_atomization_energy, compute_reference_energy
from moldesign.utils import get_platform_info
from colmena.method_server import ParslMethodServer
from colmena.redis.queue import ClientQueues, make_queue_pairs

# Define the QCMethod used for the
spec = QCInputSpecification(model=Model(method='hf', basis='sto-3g')) 
code = 'nwchem'
compute_config = {'nnodes': 2, 'cores_per_rank': 2}

multiplicity = {'H': 2, 'He': 1, 'Li': 2, 'C': 3, 'N': 4, 'O': 3, 'F': 2}


class Thinker(Thread):
    """Simple ML-enhanced optimization loop for molecular design

    Performs one simulation at a time and generates molecules in batches.
    """

    def __init__(self, queues: ClientQueues, initial_molecules: List[str], output_dir: str,
                 n_parallel: int = 1, n_molecules: int = 10):
        """
        Args:
            n_molecules (int): Number of molecules to evaluate
            output_dir (str): Path to the run directory
            initial_molecules ([str]): Initial database of molecular property data
            n_parallel (int): Maximum number of QC calculations to perform in parallel
            queues (ClientQueues): Queues for communicating with method server
        """
        super().__init__(daemon=True)
        self.database = dict()
        self.initial_molecules = initial_molecules
        self.n_evals = n_molecules
        self.n_parallel = n_parallel
        self.queues = queues
        self.logger = logging.getLogger(self.__class__.__name__)
        self.output_dir = output_dir
        self.output_file = open(os.path.join(self.output_dir, 'simulation_records.jsonld'), 'w')
        self.rl_records = open(os.path.join(self.output_dir, 'rl_records.jsonld'), 'w')
        self.screen_records = open(os.path.join(self.output_dir, 'screening_records.jsonld'), 'w')

        # Synchronization between ML and QC loops
        self._task_queue = PriorityQueue(maxsize=n_parallel * 2)
        self._ref_energies = None
        self._gen_done = Event()

    def simulation_dispatcher(self):
        """Runs the ML loop: Generate tasks for the simulator"""

        # Send out the first block
        self.logger.info('Simulation dispatcher waiting for work')
        for i in range(self.n_parallel):
            _, task = self._task_queue.get(block=True)
            self.queues.send_inputs(task, spec, self._ref_energies, compute_config,
                                    topic='simulator',
                                    method='compute_atomization_energy')
        self.logger.info('Sent out first set of tasks')

        # As they come back submit new ones
        while not self._gen_done.is_set():
            # Get the task and store its content
            result = self.queues.get_result(topic='simulator')
            self.logger.info('QC task completed')
            if result.success:
                self.database[result.args[0]] = result.value
            else:
                logging.warning('Calculation failed! See simulation outputs and Parsl log file')
            print(result.json(), file=self.output_file)
            self.output_file.flush()

            # Get a new one from the priority queue and submit it
            (step_number, _), task = self._task_queue.get()
            logging.info(f'Running {task} from batch {-step_number}')
            self.queues.send_inputs(task, spec, self._ref_energies, compute_config,
                                    topic='simulator',
                                    method='compute_atomization_energy')

        # Waiting for the still-ongoing tasks to complete
        self.logger.info('Collecting the last molecules')
        for i in range(self.n_parallel):
            # TODO (wardlt): Make a utility function for this
            # Get the task and store its content
            result = self.queues.get_result(topic='simulator')
            self.logger.info(f'Retrieved {i+1}/{self.n_parallel} on-going tasks')
            if result.success:
                self.database[result.args[0]] = result.value
            else:
                logging.warning('Calculation failed! See simulation outputs and Parsl log file')
            print(result.json(), file=self.output_file)
            self.output_file.flush()

        self.logger.info('Task consumer has completed')

    def run(self):
        # Get the reference energies
        #  TODO (wardlt): I write many of the "send out and wait" patterns, should I build a utility
        #   or is this a fundamental issue?
        self.logger.info(f'Starting Thinker process')
        elems = ['H', 'C', 'N', 'O', 'F']
        for elem in elems:
            self.queues.send_inputs(elem, spec, multiplicity[elem], method='compute_reference_energy')
        ref_energies = {}
        for _ in elems:
            result = self.queues.get_result()
            ref_energies[result.args[0]] = result.value
        self._ref_energies = ref_energies
        self.logger.info(f'Computed reference energies for: {", ".join(ref_energies.keys())}')

        # Run the initial molecules
        for mol in self.initial_molecules:
            self.queues.send_inputs(mol, spec, ref_energies, compute_config, method='compute_atomization_energy')
        for _ in self.initial_molecules:
            result = self.queues.get_result()
            if result.success:
                self.database[result.args[0]] = result.value
            else:
                logging.warning('Calculation failed! See simulation outputs and Parsl log file')
            print(result.json(), file=self.output_file)
            self.output_file.flush()
        self.logger.info(f'Computed initial population of {len(self.database)} molecules')

        # Make the initial RL agent
        agent = DQNFinalState(Molecule(reward=SklearnReward(None, maximize=False)), MorganFingerprints(),
                              q_network_dense=(1024, 512, 256), epsilon_decay=0.995)

        # Launch the "simulator" thread
        design_thread = Thread(target=self.simulation_dispatcher)
        design_thread.start()

        # Perform the design loop iteratively
        step_number = 0
        while len(self.database) < self.n_evals:
            self.logger.info(f'Generating new molecules')
            # Train the machine learning model
            gf = GroupFeaturizer()
            model = Pipeline([
                ('group', gf),
                ('lasso', BayesianRidge(normalize=True))
            ])
            database_copy = self.database.copy()
            mols, atoms = zip(*database_copy.items())
            model.fit(mols, atoms)
            self.logger.info(f'Fit a model with {len(mols)} training points and {len(gf.known_groups_)} groups')

            # Use RL to generate new molecules
            agent.env.reward_fn.model = model
            self.queues.send_inputs(agent, method='generate_molecules', topic='ML')
            result = self.queues.get_result(topic='ML')
            new_molecules, agent = result.value
            self.logger.info(f'Generated {len(new_molecules)} candidate molecules')
            print(result.json(exclude={'inputs', 'value'}), file=self.rl_records)
            self.rl_records.flush()

            # Assign them scores
            self.queues.send_inputs(model, new_molecules, method='compute_score', topic='ML')
            result = self.queues.get_result(topic='ML')
            scores = result.value
            print(result.json(exclude={'inputs'}), file=self.screen_records)
            self.screen_records.flush()
            self.logger.info(f'Assigned scores to all molecules')

            # Pick a set of calculations to run
            #   Greedy selection for now
            task_options = [{'smiles': s, 'pred_atom': e} for s, e in zip(new_molecules, scores)]
            selections = greedy_selection(task_options, self.n_parallel, lambda x: -x['pred_atom'])
            self.logger.info(f'Selected {len(selections)} new molecules')

            # Add requested simulations to the queue
            for rank, task in enumerate(selections):
                self._task_queue.put(((-step_number, rank), task['smiles']))  # Sort by recency and then by best
            step_number += 1  # Increment the loop

        self.logger.info('No longer generating new candidates')
        self._gen_done.set()


if __name__ == '__main__':
    # User inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--redishost", default="127.0.0.1",
                        help="Address at which the redis server can be reached")
    parser.add_argument("--redisport", default="6379",
                        help="Port on which redis is available")
    parser.add_argument("--parallel-guesses", default=1, type=int,
                        help="Number of calculations to maintain in parallel")
    parser.add_argument("--rl-episodes", default=10, type=int,
                        help="Number of episodes to run during the reinforcement learning pipeline")
    parser.add_argument("--search-size", default=10, type=int,
                        help="Number of new molecules to evaluate during this search")
    parser.add_argument("--initial-count", default=10, type=int,
                        help="Size of the initial population of molecules to draw from QM9")

    # Parse the arguments
    args = parser.parse_args()
    run_params = args.__dict__

    # Create an output directory with the time and run parameters
    start_time = datetime.utcnow()
    params_hash = hashlib.sha256(json.dumps(run_params).encode()).hexdigest()[:6]
    out_dir = os.path.join('runs', f'{start_time.strftime("%d%b%y-%H%M%S")}-{params_hash}')
    os.makedirs(out_dir, exist_ok=True)

    # Save the run parameters to disk
    with open(os.path.join(out_dir, 'run_params.json'), 'w') as fp:
        json.dump(run_params, fp, indent=2)
    with open(os.path.join(out_dir, 'qc_spec.json'), 'w') as fp:
        print(spec.json(), file=fp)

    # Save the platform information to disk
    host_info = get_platform_info()
    with open(os.path.join(out_dir, 'host_info.json'), 'w') as fp:
        json.dump(host_info, fp, indent=2)

    # Set up the logging
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO,
                        handlers=[logging.FileHandler(os.path.join(out_dir, 'runtime.log')),
                                  logging.StreamHandler(sys.stdout)])

    # Write the configuration
    config.run_dir = os.path.join(out_dir, 'run-info')
    parsl.load(config)

    # Save Parsl configuration
    with open(os.path.join(out_dir, 'parsl_config.txt'), 'w') as fp:
        print(str(config), file=fp)

    # Connect to the redis server
    client_queues, server_queues = make_queue_pairs(args.redishost, args.redisport, serialization_method="json",
                                                    topics=['simulator', 'ML'])

    # Apply wrappers to function to affix static settings
    my_generate_molecules = partial(generate_molecules, episodes=args.rl_episodes)
    my_generate_molecules = update_wrapper(my_generate_molecules, generate_molecules)

    # Create the method server and task generator
    ml_cfg = {'executors': ['ml']}
    dft_cfg = {'executors': ['qc']}
    doer = ParslMethodServer([(my_generate_molecules, ml_cfg), (compute_score, ml_cfg),
                              (compute_atomization_energy, dft_cfg),
                              (compute_reference_energy, dft_cfg)],
                             server_queues)

    # Select a list of initial molecules
    with open('qm9-smiles.json') as fp:
        initial_mols = np.random.choice(json.load(fp), size=(args.initial_count,), replace=False)

    thinker = Thinker(client_queues, output_dir=out_dir,
                      initial_molecules=initial_mols,
                      n_parallel=args.parallel_guesses,
                      n_molecules=args.search_size)
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
