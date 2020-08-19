"""Add some molecules"""
from qcportal.collections import OptimizationDataset
from qcportal.client import FractalClient
from qcportal import Molecule
from tqdm import tqdm
import pandas as pd
import argparse

# Hard-coded stuff
coll_name = 'Colmena Initial Dataset'


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
            'keywords': {'convergence_set': 'GAU_VERYTIGHT'}
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
            'keywords': {'convergence_set': 'GAU_VERYTIGHT'}
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
arg_parser.add_argument('qm9_path', help='Path to the QM9 data file')
arg_parser.add_argument('password', help='Password for the service')
arg_parser.add_argument('--address', help='Address to QCFractal service',
                        default='localhost:7874', type=str)
args = arg_parser.parse_args()

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

# Load in QM9
data = pd.read_json(args.qm9_path, lines=True)
data = data.sample(frac=1.0, random_state=1)
data = data.head(args.limit)

# Add the molecules to the collection
mols_to_add = [Molecule.from_data(xyz, name=smiles) for xyz, smiles in zip(data['xyz'], data['smiles_0'])]

# Submit the molecules to server
for mol in tqdm(mols_to_add):
    if mol.name not in existing_mols:
        coll.add_entry(mol.name, mol, save=False)
coll.save()

# Trigger the calculations
for spec in desired_specs:
    if spec['name'] in args.spec:
        n_started = coll.compute(spec['name'], tag='colmena')
        print(f'Started {n_started} {spec["name"]} calculations')
