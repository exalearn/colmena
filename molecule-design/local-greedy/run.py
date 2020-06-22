import argparse
import hashlib
import json
import sys
import logging
import os
from datetime import datetime
from functools import partial, update_wrapper
from threading import Thread
from typing import List

import parsl
import numpy as np
from qcelemental.models.procedures import QCInputSpecification, Model
from sklearn.linear_model import BayesianRidge
from sklearn.pipeline import Pipeline

from moldesign.config import theta_nwchem_config as config
from moldesign.sample.moldqn import generate_molecules
from moldesign.score import compute_score
from moldesign.score.group_contrib import GroupFeaturizer
from moldesign.select import greedy_selection
from moldesign.simulate import compute_atomization_energy, compute_reference_energy
from moldesign.utils import get_platform_info
from colmena.method_server import ParslMethodServer
from colmena.redis.queue import ClientQueues, make_queue_pairs

# Define the QCMethod used for the
spec = QCInputSpecification(model=Model(method='hf', basis='sto-3g')) 
compute_config = {'nnodes': 1, 'cores_per_rank': 2}


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
        super().__init__()
        self.database = dict()
        self.initial_molecules = initial_molecules
        self.n_evals = n_molecules
        self.n_parallel = n_parallel
        assert n_molecules % n_parallel == 0, "# evals must be a multiple of the number of calculations in parallel"
        self.queues = queues
        self.logger = logging.getLogger(self.__class__.__name__)
        self.output_dir = output_dir

    def run(self):
        # Output files
        output_files = {
            'simulation': open(os.path.join(self.output_dir, 'simulation_records.jsonld'), 'w')
        }

        # Get the reference energies
        #  TODO (wardlt): I write many of the "send out and wait" patterns, should I build a utility
        #   or is this a fundamental issue?
        self.logger.info(f'Starting Thinker process')
        elems = ['H', 'C', 'N', 'O', 'F']
        for elem in elems:
            self.queues.send_inputs(elem, spec, method='compute_reference_energy')
        ref_energies = {}
        for _ in elems:
            result = self.queues.get_result()
            ref_energies[result.args[0]] = result.value
        self.logger.info(f'Computed reference energies for: {", ".join(ref_energies.keys())}')

        # Run the initial molecules
        for mol in self.initial_molecules:
            self.queues.send_inputs(mol, spec, ref_energies, compute_config, method='compute_atomization_energy')
        for _ in self.initial_molecules:
            result = self.queues.get_result()
            self.database[result.args[0]] = result.value
            print(result.json(), file=output_files['simulation'])
            output_files['simulation'].flush()
        self.logger.info(f'Computed initial population of {len(self.database)} molecules')

        for i in range(self.n_evals // self.n_parallel):
            self.logger.info(f'Starting design loop step {i}')
            # Train the machine learning model
            gf = GroupFeaturizer()
            model = Pipeline([
                ('group', gf),
                ('lasso', BayesianRidge(normalize=True))
            ])
            mols, atoms = zip(*self.database.items())
            model.fit(mols, np.multiply(atoms, -1))  # negative so that the RL optimizes a positive value
            self.logger.info(f'Fit a model with {len(mols)} training points and {len(gf.known_groups_)} groups')

            # Use RL to generate new molecules
            self.queues.send_inputs(model, method='generate_molecules')
            result = self.queues.get_result()
            new_molecules = result.value
            self.logger.info(f'Generated {len(new_molecules)} candidate molecules')

            # Assign them scores
            self.queues.send_inputs(model, new_molecules, method='compute_score')
            result = self.queues.get_result()
            scores = result.value
            self.logger.info(f'Assigned scores to all molecules')

            # Pick a set of calculations to run
            #   Greedy selection for now
            task_options = [{'smiles': s, 'pred_atom': e} for s, e in zip(new_molecules, scores)]
            selections = greedy_selection(task_options, self.n_parallel, lambda x: -x['pred_atom'])
            self.logger.info(f'Selected {len(selections)} new molecules')

            # Run the selected simulations
            for task in selections:
                self.queues.send_inputs(task['smiles'], spec, ref_energies, method='compute_atomization_energy')

            # Wait for them to return
            for _ in selections:
                output = self.queues.get_result()
                self.database[output.args[0]] = output.value
                print(output.json(), file=output_files['simulation'])
                output_files['simulation'].flush()


if __name__ == '__main__':
    # User inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--redishost", default="127.0.0.1",
                        help="Address at which the redis server can be reached")
    parser.add_argument("--redisport", default="6379",
                        help="Port on which redis is available")
    parser.add_argument("--parallel_guesses", default=1, type=int,
                        help="Number of calculations to maintain in parallel")
    parser.add_argument("--rl_episodes", default=100, type=int,
                        help="Number of episodes to run during the reinforcement learning pipeline")
    parser.add_argument("--search_size", default=10, type=int,
                        help="Number of new molecules to evaluate during this search")
    parser.add_argument("--initial_count", default=10, type=int,
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
    client_queues, server_queues = make_queue_pairs(args.redishost, args.redisport, use_pickle=True)

    # Apply wrappers to function to affix static settings
    my_generate_molecules = partial(generate_molecules, episodes=args.rl_episodes)
    my_generate_molecules = update_wrapper(my_generate_molecules, generate_molecules)

    # Create the method server and task generator
    doer = ParslMethodServer([my_generate_molecules, compute_score,
                              compute_atomization_energy, compute_reference_energy],
                             server_queues, default_executors=['htex'])

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
