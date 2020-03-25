from typing import List, Dict

from sklearn.base import BaseEstimator, TransformerMixin
from rdkit import Chem
import numpy as np


def get_groups(smiles: str) -> [str]:
    """Get the group identities of each non-hydrogen atom in a molecule

    We represent group identities as a SMARTS string

    Args:
        smiles (str): SMILES string of a molecule
    Returns:
        ([str]) Identities of each atom
    """

    # Parse the molecule
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return []  # Fails to parse

    # Loop over non-H atoms
    output = []
    for a in mol.GetAtoms():
        # Get my SMARTS type
        my_type = f'[{a.GetSmarts()}D{a.GetDegree()}]'

        # Loop over bonds
        bonds = []
        for b in a.GetBonds():
            bt = str(b.GetBondType())
            other_atom = b.GetOtherAtom(a)
            other_type = f'[{other_atom.GetSmarts()}D{other_atom.GetDegree()}]'
            if bt == "SINGLE":
                bonds.append(other_type)
            elif bt == "DOUBLE":
                bonds.append(f'={other_type}')
            elif bt == "TRIPLE":
                bonds.append(f'#{other_type}')
            elif bt == "AROMATIC":
                bonds.append(f':{other_type}')
            else:
                raise ValueError(f'Unsupported bond type: {bt}')

        # Compile them into a SMARTS
        if len(bonds) > 0:
            bonds = sorted(bonds)
            my_type += ''.join(f'({x})' for x in bonds[:-1])
            my_type += bonds[-1]
        output.append(my_type)
    return output


def compute_features(smiles: str, known_types: Dict[str, int]) -> np.array:
    """Compute the feature matrices for a certain SMILES string

    Args:
        smiles (str): SMILES of target molecule
        known_types (dict): Mapping of known group types
    Returns:
        (np.array) Count of each known type of funcitonal group
    """

    # Get the groups
    groups = get_groups(smiles)

    # Get their indices
    output = np.zeros((len(known_types)), dtype=np.int)
    for g in groups:
        my_type = known_types.get(g, None)
        if my_type is not None:
            output[my_type] += 1
    return output


class GroupFeaturizer(BaseEstimator, TransformerMixin):
    """Get the count of different functional groups in a molecule to use as features"""

    def __init__(self):
        self.known_groups_ = None

    def fit(self, X, y=None):
        observed_groups = set(sum([get_groups(x) for x in X], []))
        self.known_groups_ = dict((x, i) for i, x in enumerate(sorted(observed_groups)))
        return self

    def transform(self, X, y=None):
        # Get all functional groups in the training data
        X = np.vstack(compute_features(x, self.known_groups_) for x in X)
        return X


