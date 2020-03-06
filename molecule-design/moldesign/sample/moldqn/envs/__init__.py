import copy
import logging
from functools import partial
from typing import List, Optional, Callable

import gym
import numpy as np
from gym import Space
from rdkit import Chem
from rdkit import DataStructs
from rdkit.Chem import Draw, AllChem

from .utils.molecules import get_valid_actions, utils

logger = logging.getLogger(__name__)


def _compute_canonical_smiles(smiles: str) -> str:
    """Make a SMILES string canonical

    Args:
        smiles (str): Smiles string
    Return:
        (str) Canonical smiles
    """

    mol = Chem.MolFromSmiles(smiles)
    return Chem.MolToSmiles(mol)


def compile_smiles(mol) -> str:
    """Compute the InCHi string of a RDKit molecule object

    Args:
        mol (Mol): RDKit molecule
    Returns:
        (str) InChI string
    """
    return Chem.MolToSmiles(mol)


class MoleculeActions(Space):
    """Action space for molecule design

    Generates which molecules are possible next steps and stores
    them as potential actions, following the approach of
     `Zhou et al. <http://www.nature.com/articles/s41598-019-47148-x>`_."""

    def __init__(self, atom_types, allow_removal=True, allow_no_modification=False,
                 allow_bonds_between_rings=True, allowed_ring_sizes=None,
                 fingerprint_size=2048, fingerprint_radius=3):
        """
        Args:
            atom_types: The set of elements the molecule may contain.
            state. If None, an empty molecule will be created.
            allow_removal: Boolean. Whether to allow removal of a bond.
            allow_no_modification: Boolean. If true, the valid action set will
                include doing nothing to the current molecule, i.e., the current
                molecule itself will be added to the action set.
            allow_bonds_between_rings: Boolean. If False, new bonds connecting two
                atoms which are both in rings are not allowed.
                DANGER Set this to False will disable some of the transformations eg.
                c2ccc(Cc1ccccc1)cc2 -> c1ccc3c(c1)Cc2ccccc23
                But it will make the molecules generated make more sense chemically.
            allowed_ring_sizes: Set of integers or None. The size of the ring which
                is allowed to form. If None, all sizes will be allowed. If a set is
                provided, only sizes in the set is allowed.
             fingerprint_size (int): Length of the fingerprint used to represent each molecule
             fingerprint_radius (int): Size of the radius to include for the
        """

        super().__init__((None, fingerprint_size), np.int)

        # Store the rules for defining actions
        self.atom_types = atom_types
        self.allow_removal = allow_removal
        self.allow_no_modification = allow_no_modification
        self.allow_bonds_between_rings = allow_bonds_between_rings
        self.allowed_ring_sizes = allowed_ring_sizes
        self._state = None
        self._valid_actions = []
        self._max_bonds = 4
        atom_types = list(self.atom_types)
        self._max_new_bonds = dict(
            list(zip(atom_types, utils.atom_valences(atom_types)))
        )

        # Store the function for computing features
        self.fingerprint_function = partial(compute_morgan_fingerprints,
                                            fingerprint_length=fingerprint_size,
                                            fingerprint_radius=fingerprint_radius)

        # Placeholders for action space
        self._valid_actions = []
        self._valid_actions_featurized = []

    def sample(self):
        return self.np_random.randint(0, len(self._valid_actions))

    def contains(self, x):
        return x in self._valid_actions_featurized

    @property
    def n(self):
        return len(self._valid_actions)

    def get_possible_actions(self, smiles=False):
        """Get the possible actions given the current state

        Args:
            smiles (bool): Whether to return the smiles strings, or the featurized molecules
        Returns:
            (ndarray) List of the possible actions
        """
        output = self._valid_actions if smiles else self._valid_actions_featurized
        return copy.deepcopy(output)

    def update_actions(self, new_state, allowed_space: Optional[Space] = None):
        """Generate the available actions for a new state

        Uses the actions to redefine the action space for

        Args:
            new_state (str): Molecule used to define action space
            allowed_space (Space): Space of possible observations
        """

        # Store the new state
        self._state = new_state

        # Compute the possible actions, which we describe by the new molecule they would form
        self._valid_actions = get_valid_actions(
            new_state,
            atom_types=self.atom_types,
            allow_removal=self.allow_removal,
            allow_no_modification=self.allow_no_modification,
            allowed_ring_sizes=self.allowed_ring_sizes,
            allow_bonds_between_rings=self.allow_bonds_between_rings)

        # Get only those actions which are in the desired space
        if allowed_space is not None:
            self._valid_actions = np.array([x for x in self._valid_actions
                                            if _compute_canonical_smiles(x) in allowed_space])
        else:
            self._valid_actions = np.array(list(self._valid_actions))

        # Compute the features for the next states
        self._valid_actions_featurized = np.array([self.fingerprint_function(m)
                                                   for m in self._valid_actions])

    def get_smiles_from_fingerprint(self, action):
        """Lookup the smiles string for an action given its fingerprint

        Args:
            action (ndarray): Fingerprint of a certain action
        Returns:
            (str) SMILES string associated with that action
        """

        for fingerprint, smiles in zip(self._valid_actions_featurized, self._valid_actions):
            if np.array_equal(fingerprint, action):
                return smiles
        raise ValueError('Action not found in current action space')


class Molecule(gym.Env):
    """Defines the Markov decision process of generating a molecule.

    Adapted from: https://github.com/google-research/google-research/blob/master/mol_dqn/chemgraph/dqn/molecules.py"""

    def __init__(self, target_fn: Callable[[str], float], action_space: MoleculeActions = None,
                 observation_space: Optional[Space] = None,
                 init_mol=None, max_steps=10,
                 record_path=False, fingerprint_size=2048, fingerprint_radius=3):
        """Initializes the parameters for the MDP.

        Internal state will be stored as SMILES strings, but but the environment will
        return the new state as an ML-ready fingerprint

        Args:
          init_mol: String, Chem.Mol, or Chem.RWMol. If string is provided, it is
            considered as the SMILES string. The molecule to be set as the initial
            state. If None, an empty molecule will be created.
          max_steps: Integer. The maximum number of steps to run.
          target_fn: A function or None. The function should have Args of a
            String, which is a SMILES string (the state), and Returns as
            a Boolean which indicates whether the input satisfies a criterion.
            If None, it will not be used as a criterion.
          record_path: Boolean. Whether to record the steps internally.
        """

        # Capture the user settings
        if action_space is None:
            action_space = MoleculeActions(['C', 'O', 'N', 'F'])
        self.action_space = action_space
        self.init_mol = init_mol
        self.max_steps = max_steps
        self.target_fn = target_fn
        self.record_path = record_path
        self.observation_space = observation_space

        # Store the function used to compute inputs
        self.fingerprint_function = partial(compute_morgan_fingerprints,
                                            fingerprint_length=fingerprint_size,
                                            fingerprint_radius=fingerprint_radius)

        # Define the state variables
        self._state = None
        self._state_fingerprint = None
        self._path = None
        self._counter = None

        # Ready the environment
        self.reset()

    @property
    def num_steps_taken(self):
        return self._counter

    @property
    def state(self):
        """State as a SMILES string"""
        return self._state

    def get_path(self):
        return list(self._path)

    def reset(self):
        """Resets the MDP to its initial state."""
        self._state = self.init_mol
        self._state_fingerprint = self.fingerprint_function(self._state)
        self.action_space.update_actions(self._state, self.observation_space)
        if self.record_path:
            self._path = [self._state]
        self._counter = 0

    def _reward(self):
        """Gets the reward for the state.

        A child class can redefine the reward function if reward other than
        zero is desired.

        Returns:
          Float. The reward for the current state.
        """
        return self.target_fn(self.state)

    def step(self, action):
        """Takes a step forward according to the action.

        Args:
          action (ndarray): Fingerprint of action

        Raises:
          ValueError: If the number of steps taken exceeds the preset max_steps, or
            the action is not in the set of valid_actions.

        """
        if self._counter >= self.max_steps:
            raise ValueError('This episode is terminated.')

        # Get the SMILES string associated with this action
        self._state = self.action_space.get_smiles_from_fingerprint(action)
        if self.record_path:
            self._path.append(self._state)

        # Store the fingerprint of the state
        self._state_fingerprint = self.fingerprint_function(self._state)

        # Update the action space
        self.action_space.update_actions(self._state, self.observation_space)
        self._counter += 1

        # Check if we have finished
        #  Out of steps or no more moves
        done = ((self._counter >= self.max_steps) or
                len(self.action_space.get_possible_actions(smiles=True)) == 0)

        # Compute the fingerprints for the state
        return self._state_fingerprint, self._reward(), done, {}

    def render(self, mode='human', **kwargs):
        """Draws the molecule of the state.

        Args:
          **kwargs: The keyword arguments passed to Draw.MolToImage.

        Returns:
          A PIL image containing a drawing of the molecule.
        """
        return Draw.MolToImage(self._state, **kwargs)


def compute_morgan_fingerprints(smiles, fingerprint_length, fingerprint_radius):
    """Get Morgan Fingerprint of a specific SMILES string.

    Adapted from: <https://github.com/google-research/google-research/blob/
    dfac4178ccf521e8d6eae45f7b0a33a6a5b691ee/mol_dqn/chemgraph/dqn/deep_q_networks.py#L750>

    Args:
      smiles: String. The SMILES string of the molecule.
      fingerprint_length (int): Bit-length of fingerprint
      fingerprint_radius (int): Radius used to compute fingerprint
    Returns:
      np.array. shape = [hparams.fingerprint_length]. The Morgan fingerprint.
    """
    if smiles is None:  # No smiles string
        return np.zeros((fingerprint_length,))
    molecule = Chem.MolFromSmiles(smiles)
    if molecule is None:  # Invalid smiles string
        return np.zeros((fingerprint_length,))

    # Compute the fingerprint
    fingerprint = AllChem.GetMorganFingerprintAsBitVect(
        molecule, fingerprint_radius, fingerprint_length)
    arr = np.zeros((1,))

    # ConvertToNumpyArray takes ~ 0.19 ms, while
    # np.asarray takes ~ 4.69 ms
    DataStructs.ConvertToNumpyArray(fingerprint, arr)
    return arr
