"""Functions related to simulating molecular properties"""

from typing import Dict

from qcelemental.models import AtomicInput, OptimizationInput, Molecule
from qcelemental.models.procedures import QCInputSpecification
from qcengine import compute_procedure, compute
from rdkit import Chem
from rdkit.Chem import AllChem

from openbabel import OBConformerSearch, OBForceField
from openbabel.pybel import readstring
from openbabel import pybel


def generate_atomic_coordinates(smiles) -> str:
    """Attempt to further refine the molecular structure through a rotor search
    Code adapted from: http://forums.openbabel.org/OpenBabel-Conformer-Search-td4177357.html
    Args:
        smiles (string): Smiles string of molecule to be generated
    Returns:
        (string): XYZ coordinates of molecule
    """

    # Convert it to a OpenBabel molecule
    mol = readstring('smi', smiles)

    # Generate initial 3D coordinates
    mol.make3D()

    # Try to get a forcefield that works with this molecule
    ff = _get_forcefield(mol)

    # initial cleanup before the weighted search
    ff.SteepestDescent(500, 1.0e-4)
    ff.WeightedRotorSearch(100, 20)
    ff.ConjugateGradients(500, 1.0e-6)
    ff.GetCoordinates(mol.OBMol)

    return mol.write("xyz")


def _get_forcefield(mol: Molecule) -> OBForceField:
    """Given a molecule, get a suitable forcefield
    Args:
        mol (Molecule): Molecule to be initialized
    Returns:
        (OBForcefield): Forcefield initialized with the molecule
    """
    ff = pybel._forcefields["mmff94"]
    success = ff.Setup(mol.OBMol)
    if not success:
        ff = pybel._forcefields["uff"]
        success = ff.Setup(mol.OBMol)
        if not success:
            raise Exception('Forcefield setup failed')

    return ff


def compute_atomization_energy(smiles: str, qc_config: QCInputSpecification, reference_energies: Dict[int, float]) -> float:
    """Compute the atomization energy of a molecule given the SMILES string

    Args:
        smiles (str): SMILES of a molecule
        qc_config (dict): Quantum Chemistry configuration used for evaluating the energy
        reference_energies (dict): Reference energies for each element
    Returns:
        (float): Atomization energy of this molecule
    """

    # Generate 3D coordinates by minimizing MMFF forcefield
    xyz = generate_atomic_coordinates(smiles)
    mol = Molecule.from_data(xyz, dtype='xyz')

    # Run the relaxation
    opt_input = dict(input_specification=qc_config, initial_molecule=mol, keywords={'program': 'psi4'})
    res = compute_procedure(opt_input, 'geometric', raise_error=True)
    return res
