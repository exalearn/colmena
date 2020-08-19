"""Test out the MPNN models"""
from typing import List

from molgym.mpnn.layers import Squeeze, GraphNetwork
from tensorflow.keras.layers import Input, Dense
from tensorflow.keras import Model
import numpy as np
import pytest

from moldesign.score.mpnn import evaluate_mpnn, update_mpnn, MPNNMessage


@pytest.fixture
def model():
    node_graph_indices = Input(shape=(1,), name='node_graph_indices', dtype='int32')
    atom_types = Input(shape=(1,), name='atom', dtype='int32')
    bond_types = Input(shape=(1,), name='bond', dtype='int32')
    connectivity = Input(shape=(2,), name='connectivity', dtype='int32')

    # Squeeze the node graph and connectivity matrices
    snode_graph_indices = Squeeze(axis=1)(node_graph_indices)
    satom_types = Squeeze(axis=1)(atom_types)
    sbond_types = Squeeze(axis=1)(bond_types)

    output = GraphNetwork(3, 5, 64, 1, output_layer_sizes=[32],
                          atomic_contribution=True, reduce_function='sum',
                          name='mpnn')([satom_types, sbond_types, snode_graph_indices, connectivity])

    # Scale the output
    output = Dense(1, activation='linear', name='scale')(output)

    return Model(inputs=[node_graph_indices, atom_types, bond_types, connectivity],
                 outputs=output)


@pytest.fixture
def atom_types() -> List[int]:
    return [1, 6]


@pytest.fixture
def bond_types() -> List[str]:
    return ["AROMATIC", "DOUBLE", "SINGLE", "TRIPLE"]


def test_evaluate(model, atom_types, bond_types):
    smiles = ['CC', 'CCC', 'CC=C', 'CCC']
    output = evaluate_mpnn([MPNNMessage(model), MPNNMessage(model)], smiles, atom_types, bond_types, batch_size=2)
    assert output.shape == (4, 2)
    assert np.isclose(output[1, :], output[3, :], atol=1e-6).all()


def test_training(model, atom_types, bond_types):
    smiles = ['CC', 'CCCC', 'CC=C', 'CCC']
    y = [1, 2, 3, 4]
    model.compile('adam', 'mean_squared_error')
    new_weights, history = update_mpnn(MPNNMessage(model), dict(zip(smiles, y)),
                                       4, atom_types, bond_types, validation_split=0.5)
    model.set_weights(new_weights)
    assert len(history['loss']) == 4
