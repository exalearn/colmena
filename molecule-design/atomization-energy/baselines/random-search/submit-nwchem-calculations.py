"""Add some molecules from GDB13"""
from moldesign.simulate import generate_atomic_coordinates
from multiprocessing import Pool

from qcelemental import MoleculeFormatError, ValidationError
from qcportal.collections import OptimizationDataset
from qcportal.client import FractalClient
from qcelemental.models import Molecule
from tqdm import tqdm
import numpy as np
import argparse
import gzip
import os

# Hard-coded stuff
coll_name = 'GDB13 Random Sample'


def create_specs(client: FractalClient):
    """Make the desired specifications for our tests
    
    Requires using keyword sets, hence the need for the client.
    
    Args:
        client (FractalClient): Client used for making found_specs
    """

    # Make the keyword sets
    xfine_kwds = client.add_keywords([
        {'values': {
            'dft__convergence__energy': '1e-8',
            'dft__grid': 'xfine',
            'basis__spherical': True
        }, 'comments': 'Tight convergence settings for NWChem'},
    ])[0]

    # Return the specifications
    return [{
        'name': 'small_basis',
        'description': 'Geometric + NWChem/B3LYP/3-21g',
        'optimization_spec': {
            'program': 'geometric',
        }, 'qc_spec': {
            'driver': 'gradient',
            'method': 'b3lyp',
            'basis': '3-21g',
            'program': 'nwchem',
            'keywords': xfine_kwds
        }
    }, {
        'name': 'default',
        'description': 'Geometric + NWChem/B3LYP/6-31g(2df,p)',
        'optimization_spec': {
            'program': 'geometric',
        }, 'qc_spec': {
            'driver': 'gradient',
            'method': 'b3lyp',
            'basis': '6-31g(2df,p)',
            'program': 'nwchem',
            'keywords': xfine_kwds
        }
    }]


# Parse input arguments
arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--spec', help='Specification for the QC to run', default=['small_basis'], nargs='*', type=str)
arg_parser.add_argument('--limit', help='Number of molecules to add. -1 to add all', default=1, type=int)
arg_parser.add_argument('--gdb13', help='Path to the gdb13 data file',
                        default=os.path.join('..', '..', '..', 'search-spaces', 'G13.csv.gz'))
arg_parser.add_argument('password', help='Password for the service')
arg_parser.add_argument('--address', help='Address to QCFractal service',
                        default='localhost:7874', type=str)
args = arg_parser.parse_args()

# Pick some molecules by random selection
with gzip.open(args.gdb13, 'rt') as fp:
    count = 0
    for _ in tqdm(fp):
        count += 1
print(f'Found {count} molecules in search space')
rng = np.random.RandomState(1)
choices = set()
while len(choices) < args.limit:
    choices.add(rng.randint(count))
choices = np.sort(list(choices))

# Select molecules using an out-of-core strategy
bar = tqdm(total=len(choices))
with gzip.open(args.gdb13, 'rt') as fp:
    # Get the first molecule
    mols = []
    for _, line in zip(range(choices[0]+1), fp):
        pass
    mols.append(line.strip().split(",")[-1])
    bar.update(1)

    # Get the remaining molecules
    for n in np.diff(choices):
        for _, line in zip(range(n), fp):
            pass
        mols.append(line.strip().split(",")[-1])
        bar.update(1)

print(f'Selected {len(choices)}: {mols[:3]}, ...')

# Make the FractalClient
client = FractalClient(args.address, verify=False, username='user', password=args.password)

# Assemble the dataset
try:
    coll = OptimizationDataset.from_server(name=coll_name, client=client)
except KeyError:
    coll = OptimizationDataset(name=coll_name, client=client)
    coll.save()

# Make sure it has the right calculation specs
found_specs = coll.list_specifications(description=False)
print(f'Found the following specifications: {found_specs}')
desired_specs = create_specs(client)
for spec in desired_specs:
    if spec["name"] not in found_specs:
        coll.add_specification(**spec)
coll.save()

# Get the current list of molecules
existing_mols = coll.df.index

# Generate initial geometries for those not in initial dataset
mols_to_add = []
failed = []
for s in tqdm(mols):
    if s in existing_mols:
        continue

    try:
        coords = generate_atomic_coordinates(s)
        mol = Molecule.from_data(coords, dtype='xyz', name=s)
    except (MoleculeFormatError, ValidationError):
        failed += s
        continue
    mols_to_add.append(mol)

print(f'Generated initial coordinates for {len(mols_to_add)} not already in database. {len(failed)} failed.')

# Submit the molecules to server
for mol in tqdm(mols_to_add):
    if mol.name not in existing_mols:
        coll.add_entry(mol.name, mol, save=False)
coll.save()

# Trigger the calculations
for spec in desired_specs:
    if spec['name'] in args.spec:
        n_started = coll.compute(spec['name'], tag='colmena_g13')
        print(f'Started {n_started} {spec["name"]} calculations')
