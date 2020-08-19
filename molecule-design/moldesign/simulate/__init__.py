"""Functions related to simulating molecular properties"""

from typing import Dict, Optional, Union, Tuple

from uuid import uuid4
from moldesign.simulate.thermo import compute_zpe
from qcelemental.molutil import guess_connectivity
from qcelemental.models import OptimizationInput, Molecule, AtomicInput, OptimizationResult, AtomicResult
from qcelemental.models.procedures import QCInputSpecification
from qcengine import compute_procedure, compute

from openbabel import OBConformerSearch, OBForceField
from openbabel.pybel import readstring
from openbabel import pybel
from qcengine.config import TaskConfig

_code = 'nwchem'


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


def compute_atomization_energy(smiles: str, qc_config: QCInputSpecification,
                               reference_energies: Dict[str, float],
                               compute_config: Optional[Union[TaskConfig, Dict]] = None,
                               restart: bool = False,
                               compute_hessian: bool = True,
                               code: str = _code) -> Tuple[float, OptimizationResult, Optional[AtomicResult]]:
    """Compute the atomization energy of a molecule given the SMILES string

    Args:
        smiles (str): SMILES of a molecule
        qc_config (dict): Quantum Chemistry configuration used for evaluating the energy
        reference_energies (dict): Reference energies for each element
        compute_config (TaskConfig): Configuration for the quantum chemistry code
        restart (bool): Re-use the electronic configuration from the previous calculation
        compute_hessian (bool): Whether to compute the Hessian and include ZPE in total energy
        code (str): Which QC code to use for the evaluation
    Returns:
        (float): Atomization energy of this molecule
        (OptimizationResult): Output from the relaxation calculation
        (AtomicResult): Hessian calculation
    """

    # If needed, make a restart name
    if restart:
        qc_config = qc_config.copy()
        qc_config.keywords['restart_name'] = f"{smiles}_{str(uuid4())}"

    # Relax the structure
    xyz, total_energy, relax_result = relax_structure(smiles, qc_config, compute_config, code=code)
    mol = relax_result.final_molecule

    # If desired, compute the hessian
    hess_result = None
    if compute_hessian:
        hess_input = AtomicInput(molecule=relax_result.final_molecule, driver='hessian',
                                 **qc_config.dict(exclude={'driver'}))
        hess_result = compute(hess_input, 'nwchem', local_options=compute_config)

        # Add the ZPE to the total energy
        zpe = compute_zpe(hess_result.return_result, mol)
        total_energy += zpe

    return subtract_reference_energies(total_energy, mol, reference_energies), relax_result, hess_result


def subtract_reference_energies(total_energy: float, mol: Molecule, reference_energies: Dict[str, float]) -> float:
    """Compute the atomization energy by subtracting off reference energies

    Args:
        total_energy (float): Total energy of a molecule
        mol (Molecule): Molecule of interest
        reference_energies (float): Isolated atom energies in the same units as total_energy
    Returns:
        (float) Atomization energy
    """
    # Get the energy of the relaxed structure
    atom_energy = total_energy
    for label in mol.symbols:
        atom_energy -= reference_energies[label]

    # Get the output energy
    return atom_energy


def relax_structure(smiles: str,
                    qc_config: QCInputSpecification,
                    compute_config: Optional[Union[TaskConfig, Dict]] = None,
                    compute_connectivity: bool = False,
                    code: str = _code) -> Tuple[str, float, OptimizationResult]:
    """Compute the atomization energy of a molecule given the SMILES string

    Args:
        smiles (str): SMILES of a molecule
        qc_config (dict): Quantum Chemistry configuration used for evaluating the energy
        compute_config (TaskConfig): Configuration for the quantum chemistry code
        compute_connectivity (bool): Whether we must compute connectivity before calling code
        code (str): Which QC code to use for the evaluation
    Returns:
        (str): Structure of the molecule
        (float): Electronic energy of this molecule
        (OptimizationResult): Full output from the calculation
    """
    # Generate 3D coordinates by minimizing MMFF forcefield
    xyz = generate_atomic_coordinates(smiles)
    mol = Molecule.from_data(xyz, dtype='xyz')

    # Generate connectivity, if needed
    if compute_connectivity:
        conn = guess_connectivity(mol.symbols, mol.geometry, default_connectivity=1.0)
        mol = Molecule.from_data({**mol.dict(), 'connectivity': conn})

    # Run the relaxation
    opt_input = OptimizationInput(input_specification=qc_config,
                                  initial_molecule=mol,
                                  keywords={'program': code})
    res = compute_procedure(opt_input, 'geometric', local_options=compute_config, raise_error=True)

    return res.final_molecule.to_string('xyz'), res.energies[-1], res


def compute_reference_energy(element: str, qc_config: QCInputSpecification,
                             n_open: int, code: str = _code) -> float:
    """Compute the energy of an isolated atom in vacuum

    Args:
        element (str): Symbol of the element
        qc_config (QCInputSpecification): Quantum Chemistry configuration used for evaluating he energy
        n_open (int): Number of open atomic orbitals
        code (str): Which QC code to use for the evaluation
    Returns:
        (float): Energy of the isolated atom
    """

    # Make the molecule
    xyz = f'1\n{element}\n{element} 0 0 0'
    mol = Molecule.from_data(xyz, dtype='xyz', molecular_multiplicity=n_open, molecular_charge=0)

    # Run the atomization energy calculation
    input_spec = AtomicInput(molecule=mol, driver='energy', **qc_config.dict(exclude={'driver'}))
    result = compute(input_spec, code, raise_error=True)

    return result.return_result
