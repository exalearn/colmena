"""Simple tests for the MolDQN system"""

from moldesign.sample.moldqn import generate_molecules


def test_moldqn():
    output = generate_molecules(len)
    assert 'C' in output  # There is a (0.25)^10 chance that this will fail
