# coding=utf-8
# Copyright 2019 The Google Research Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Lint as: python2, python3
"""Defines the Markov decision process of generating a molecule.

The problem of molecule generation as a Markov decision process, the
state space, action space, and reward function are defined.

Adapted from: https://github.com/google-research/google-research/blob/master/mol_dqn/chemgraph/dqn/molecules.py
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import copy
import itertools

from rdkit import Chem
from six.moves import range

from . import utils


class Result(
    collections.namedtuple('Result', ['state', 'reward', 'terminated'])):
    """A namedtuple defines the result of a step for the molecule class.

      The namedtuple contains the following fields:
        state: Chem.RWMol. The molecule reached after taking the action.
        reward: Float. The reward get after taking the action.
        terminated: Boolean. Whether this episode is terminated.
    """


def get_valid_actions(state, atom_types, allow_removal, allow_no_modification,
                      allowed_ring_sizes, allow_bonds_between_rings):
    """Computes the set of valid actions for a given state.

    Args:
      state: String SMILES; the current state. If None or the empty string, we
        assume an "empty" state with no atoms or bonds.
      atom_types: Set of string atom types, e.g. {'C', 'O'}.
      allow_removal: Boolean whether to allow actions that remove atoms and bonds.
      allow_no_modification: Boolean whether to include a "no-op" action.
      allowed_ring_sizes: Set of integer allowed ring sizes; used to remove some
        actions that would create rings with disallowed sizes.
      allow_bonds_between_rings: Boolean whether to allow actions that add bonds
        between atoms that are both in rings.

    Returns:
      Set of string SMILES containing the valid actions (technically, the set of
      all states that are acceptable from the given state).

    Raises:
      ValueError: If state does not represent a valid molecule.
    """
    if not state:
        # Available actions are adding a node of each type.
        return copy.deepcopy(atom_types)
    mol = Chem.MolFromSmiles(state)
    if mol is None:
        raise ValueError('Received invalid state: %s' % state)
    atom_valences = {
        atom_type: utils.atom_valences([atom_type])[0]
        for atom_type in atom_types
    }
    atoms_with_free_valence = {}
    for i in range(1, max(atom_valences.values())):
        # Only atoms that allow us to replace at least one H with a new bond are
        # enumerated here.
        atoms_with_free_valence[i] = [
            atom.GetIdx() for atom in mol.GetAtoms() if atom.GetNumImplicitHs() >= i
        ]
    valid_actions = set()
    valid_actions.update(
        _atom_addition(
            mol,
            atom_types=atom_types,
            atom_valences=atom_valences,
            atoms_with_free_valence=atoms_with_free_valence))
    valid_actions.update(
        _bond_addition(
            mol,
            atoms_with_free_valence=atoms_with_free_valence,
            allowed_ring_sizes=allowed_ring_sizes,
            allow_bonds_between_rings=allow_bonds_between_rings))
    if allow_removal:
        valid_actions.update(_bond_removal(mol))
    if allow_no_modification:
        valid_actions.add(Chem.MolToSmiles(mol))
    return valid_actions


def _atom_addition(state, atom_types, atom_valences, atoms_with_free_valence):
    """Computes valid actions that involve adding atoms to the graph.

    Actions:
      * Add atom (with a bond connecting it to the existing graph)

    Each added atom is connected to the graph by a bond. There is a separate
    action for connecting to (a) each existing atom with (b) each valence-allowed
    bond type. Note that the connecting bond is only of type single, double, or
    triple (no aromatic bonds are added).

    For example, if an existing carbon atom has two empty valence positions and
    the available atom types are {'C', 'O'}, this section will produce new states
    where the existing carbon is connected to (1) another carbon by a double bond,
    (2) another carbon by a single bond, (3) an oxygen by a double bond, and
    (4) an oxygen by a single bond.

    Args:
      state: RDKit Mol.
      atom_types: Set of string atom types.
      atom_valences: Dict mapping string atom types to integer valences.
      atoms_with_free_valence: Dict mapping integer minimum available valence
        values to lists of integer atom indices. For instance, all atom indices in
        atoms_with_free_valence[2] have at least two available valence positions.

    Returns:
      Set of string SMILES; the available actions.
    """
    bond_order = {
        1: Chem.BondType.SINGLE,
        2: Chem.BondType.DOUBLE,
        3: Chem.BondType.TRIPLE,
    }
    atom_addition = set()
    for i in bond_order:
        for atom in atoms_with_free_valence[i]:
            for element in atom_types:
                if atom_valences[element] >= i:
                    new_state = Chem.RWMol(state)
                    idx = new_state.AddAtom(Chem.Atom(element))
                    new_state.AddBond(atom, idx, bond_order[i])
                    sanitization_result = Chem.SanitizeMol(new_state, catchErrors=True)
                    # When sanitization fails
                    if sanitization_result:
                        continue
                    atom_addition.add(Chem.MolToSmiles(new_state))
    return atom_addition


def _bond_addition(state, atoms_with_free_valence, allowed_ring_sizes,
                   allow_bonds_between_rings):
    """Computes valid actions that involve adding bonds to the graph.

    Actions (where allowed):
      * None->{single,double,triple}
      * single->{double,triple}
      * double->{triple}

    Note that aromatic bonds are not modified.

    Args:
      state: RDKit Mol.
      atoms_with_free_valence: Dict mapping integer minimum available valence
        values to lists of integer atom indices. For instance, all atom indices in
        atoms_with_free_valence[2] have at least two available valence positions.
      allowed_ring_sizes: Set of integer allowed ring sizes; used to remove some
        actions that would create rings with disallowed sizes.
      allow_bonds_between_rings: Boolean whether to allow actions that add bonds
        between atoms that are both in rings.

    Returns:
      Set of string SMILES; the available actions.
    """
    bond_orders = [
        None,
        Chem.BondType.SINGLE,
        Chem.BondType.DOUBLE,
        Chem.BondType.TRIPLE,
    ]
    bond_addition = set()
    for valence, atoms in atoms_with_free_valence.items():
        for atom1, atom2 in itertools.combinations(atoms, 2):
            # Get the bond from a copy of the molecule so that SetBondType() doesn't
            # modify the original state.
            bond = Chem.Mol(state).GetBondBetweenAtoms(atom1, atom2)
            new_state = Chem.RWMol(state)
            # Kekulize the new state to avoid sanitization errors; note that bonds
            # that are aromatic in the original state are not modified (this is
            # enforced by getting the bond from the original state with
            # GetBondBetweenAtoms()).
            Chem.Kekulize(new_state, clearAromaticFlags=True)
            if bond is not None:
                if bond.GetBondType() not in bond_orders:
                    continue  # Skip aromatic bonds.
                idx = bond.GetIdx()
                # Compute the new bond order as an offset from the current bond order.
                bond_order = bond_orders.index(bond.GetBondType())
                bond_order += valence
                if bond_order < len(bond_orders):
                    idx = bond.GetIdx()
                    bond.SetBondType(bond_orders[bond_order])
                    new_state.ReplaceBond(idx, bond)
                else:
                    continue
            # If do not allow new bonds between atoms already in rings.
            elif (not allow_bonds_between_rings and
                  (state.GetAtomWithIdx(atom1).IsInRing() and
                   state.GetAtomWithIdx(atom2).IsInRing())):
                continue
            # If the distance between the current two atoms is not in the
            # allowed ring sizes
            elif (allowed_ring_sizes is not None and
                  len(Chem.rdmolops.GetShortestPath(
                      state, atom1, atom2)) not in allowed_ring_sizes):
                continue
            else:
                new_state.AddBond(atom1, atom2, bond_orders[valence])
            sanitization_result = Chem.SanitizeMol(new_state, catchErrors=True)
            # When sanitization fails
            if sanitization_result:
                continue
            bond_addition.add(Chem.MolToSmiles(new_state))
    return bond_addition


def _bond_removal(state):
    """Computes valid actions that involve removing bonds from the graph.

    Actions (where allowed):
      * triple->{double,single,None}
      * double->{single,None}
      * single->{None}

    Bonds are only removed (single->None) if the resulting graph has zero or one
    disconnected atom(s); the creation of multi-atom disconnected fragments is not
    allowed. Note that aromatic bonds are not modified.

    Args:
      state: RDKit Mol.

    Returns:
      Set of string SMILES; the available actions.
    """
    bond_orders = [
        None,
        Chem.BondType.SINGLE,
        Chem.BondType.DOUBLE,
        Chem.BondType.TRIPLE,
    ]
    bond_removal = set()
    for valence in [1, 2, 3]:
        for bond in state.GetBonds():
            # Get the bond from a copy of the molecule so that SetBondType() doesn't
            # modify the original state.
            bond = Chem.Mol(state).GetBondBetweenAtoms(bond.GetBeginAtomIdx(),
                                                       bond.GetEndAtomIdx())
            if bond.GetBondType() not in bond_orders:
                continue  # Skip aromatic bonds.
            new_state = Chem.RWMol(state)
            # Kekulize the new state to avoid sanitization errors; note that bonds
            # that are aromatic in the original state are not modified (this is
            # enforced by getting the bond from the original state with
            # GetBondBetweenAtoms()).
            Chem.Kekulize(new_state, clearAromaticFlags=True)
            # Compute the new bond order as an offset from the current bond order.
            bond_order = bond_orders.index(bond.GetBondType())
            bond_order -= valence
            if bond_order > 0:  # Downgrade this bond.
                idx = bond.GetIdx()
                bond.SetBondType(bond_orders[bond_order])
                new_state.ReplaceBond(idx, bond)
                sanitization_result = Chem.SanitizeMol(new_state, catchErrors=True)
                # When sanitization fails
                if sanitization_result:
                    continue
                bond_removal.add(Chem.MolToSmiles(new_state))
            elif bond_order == 0:  # Remove this bond entirely.
                atom1 = bond.GetBeginAtom().GetIdx()
                atom2 = bond.GetEndAtom().GetIdx()
                new_state.RemoveBond(atom1, atom2)
                sanitization_result = Chem.SanitizeMol(new_state, catchErrors=True)
                # When sanitization fails
                if sanitization_result:
                    continue
                smiles = Chem.MolToSmiles(new_state)
                parts = sorted(smiles.split('.'), key=len)
                # We define the valid bond removing action set as the actions
                # that remove an existing bond, generating only one independent
                # molecule, or a molecule and an atom.
                if len(parts) == 1 or len(parts[0]) == 1:
                    bond_removal.add(parts[-1])
    return bond_removal
