"""Simple tests for the MolDQN system"""

from moldesign.sample.moldqn import generate_molecules
from sklearn.base import BaseEstimator


class FakeEstimator(BaseEstimator):
    def predict(self, X):
        return [0] * len(X)


def test_moldqn():
    output = generate_molecules(FakeEstimator())
    assert 'C' in output  # There is a (0.25)^10 chance that this will fail
