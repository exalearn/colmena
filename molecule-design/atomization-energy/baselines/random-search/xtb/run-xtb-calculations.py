"""Add some molecules from GDB13"""
import json
from functools import partial

from moldesign.simulate import compute_atomization_energy
from parsl import HighThroughputExecutor
from parsl.addresses import address_by_hostname
from parsl.app.python import PythonApp

from concurrent.futures import as_completed

from parsl.config import Config
from parsl.launchers import AprunLauncher
from parsl.providers import CobaltProvider
from tqdm import tqdm
import pandas as pd
import numpy as np
import argparse
import parsl
import gzip
import os

xtb_dir = os.path.join('..', '..', '..', 'notebooks', 'xtb')

# Parse input arguments
arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--out-file', help='Output path for the results', default='xtb-random-search.jsonld', type=str)
arg_parser.add_argument('--run-size', help='Number of molecules to add. -1 to add all', default=1, type=int)
arg_parser.add_argument('--limit', help='Total number of molecules to run', default=1, type=int)
arg_parser.add_argument('--gdb13', help='Path to the gdb13 data file',
                        default=os.path.join('..', '..', '..', '..', 'search-spaces', 'G13.csv.gz'))
args = arg_parser.parse_args()

# Pick some molecules by random selection
with gzip.open(args.gdb13, 'rt') as fp:
    count = 0
    for _ in tqdm(zip(fp, range(1000))):
        count += 1
print(f'Found {count} molecules in search space')
rng = np.random.RandomState(2)
choices = set()
while len(choices) < args.limit:
    choices.add(rng.randint(count))
choices = np.sort(list(choices))

# Select molecules using an out-of-core strategy
bar = tqdm(total=len(choices))
with gzip.open(args.gdb13, 'rt') as fp:
    # Get the first molecule
    mols = []
    for _, line in zip(range(choices[0] + 1), fp):
        pass
    mols.append(line.strip().split(",")[-1])
    bar.update(1)

    # Get the remaining molecules
    for n in np.diff(choices):
        for _, line in zip(range(n), fp):
            pass
        mols.append(line.strip().split(",")[-1])
        bar.update(1)

print(f'Selected {len(choices)}: {", ".join(mols[:3])}, ...')

# Remove already-computed molecules
if os.path.isfile(args.out_file):
    old_mols = pd.read_json(args.out_file, lines=True)['smiles'].tolist()
    mols = list(set(mols).difference(old_mols))
    print(f'Removed already-complete records. New count: {len(mols)}')

# Make the Parsl configuration
config = Config(
    retries=1,
    executors=[
        HighThroughputExecutor(
            address=address_by_hostname(),
            label="qc",
            max_workers=4,
            provider=CobaltProvider(
                cmd_timeout=120,
                nodes_per_block=8,
                account='CSC249ADCD08',
                queue='debug-cache-quad',
                walltime="1:00:00",
                init_blocks=1,
                max_blocks=1,
                launcher=AprunLauncher(overrides='-d 64 -j 1'),  # Places worker on the launch node
                scheduler_options='#COBALT --attrs enable_ssh=1',
                worker_init='''
module load miniconda-3
conda activate /lus/theta-fs0/projects/CSC249ADCD08/colmena/env
which python
''',
            ),
        )
    ]
)
parsl.load(config)

# Make the input function
with open(os.path.join(xtb_dir, 'qc_config.json')) as fp:
    qc_config = json.load(fp)
    qc_config.pop('program')
with open(os.path.join(xtb_dir, 'ref_energies.json')) as fp:
    ref_energies = json.load(fp)
func = PythonApp(
    partial(compute_atomization_energy, qc_config=qc_config, reference_energies=ref_energies,
            compute_hessian=False, code='xtb')
)

# Submit all of the molecules to be run
jobs = [func(m) for m in mols]

# Receive the results
n_failed = 0
with open(args.out_file, 'a') as fp:
    for result in tqdm(as_completed(jobs), total=len(jobs)):
        try:
            energy, _, _ = result.result()
        except BaseException:
            n_failed += 1
            continue

        # Save the output
        smiles = result.task_def['args'][0]
        print(json.dumps({'smiles': smiles, 'energy': energy}), file=fp)
print(f'{len(jobs) - n_failed} tasks succeeded. {n_failed} failed')
