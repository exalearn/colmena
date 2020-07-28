"""Functions for updating and performing bulk inference using an Keras MPNN model"""
from typing import List

import numpy as np
import tensorflow as tf
from molgym.mpnn.data import convert_nx_to_dict
from molgym.utils.conversions import convert_smiles_to_nx


def _merge_batch(mols: List[dict]) -> dict:
    """Merge a list of molecules into a single batch

    Args:
        mols: List of molecules in dictionary format
    Returns:
        Single batch of molecules
    """

    # Convert arrays to array

    # Stack the values from each array
    batch = dict(
        (k, np.concatenate([np.atleast_1d(m[k]) for m in mols], axis=0))
        for k in mols[0].keys()
    )

    # Compute the mappings from bond index to graph index
    batch_size = len(mols)
    mol_id = np.arange(batch_size, dtype=np.int)
    batch['node_graph_indices'] = np.repeat(mol_id, batch['n_atom'], axis=0)
    batch['bond_graph_indices'] = np.repeat(mol_id, batch['n_bond'], axis=0)

    # Compute offsets for the connectivity matrix
    offset_values = np.zeros(batch_size, dtype=np.int)
    np.cumsum(batch['n_atom'][:-1], out=offset_values[1:])
    offsets = np.repeat(offset_values, batch['n_bond'], axis=0)
    batch['connectivity'] += np.expand_dims(offsets, 1)

    return batch


def evaluate_mpnn(model: tf.keras.Model, smiles: List[str], atom_types: List[int], bond_types: List[str],
                  batch_size: int = 128) -> np.ndarray:
    """Run inference on a list of molecules

    Args:
        model: Model to use to run inference
        smiles: List of molecules to evaluate
        batch_size: List of molecules to create into matches
    Returns:
        Predicted value for each molecule
    """

    # Convert all SMILES strings to batches of molecules
    # TODO (wardlt): Use multiprocessing. Could benefit from a persistent Pool to avoid loading in TF many times
    mols = [convert_nx_to_dict(convert_smiles_to_nx(s), atom_types, bond_types) for s in smiles]
    chunks = [mols[start:start + batch_size] for start in range(0, len(mols), batch_size)]
    batches = [_merge_batch(c) for c in chunks]

    # Feed the batches through the MPNN
    outputs = [model.predict_on_batch(b) for b in batches]
    return np.vstack(outputs)
