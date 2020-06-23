"""Simple tests for the MolDQN system"""

from moldesign.sample.moldqn import generate_molecules, SklearnReward
from molgym.agents.moldqn import DQNFinalState
from molgym.agents.preprocessing import MorganFingerprints
from molgym.envs.simple import Molecule
from sklearn.base import BaseEstimator


class FakeEstimator(BaseEstimator):
    def predict(self, X):
        return [0] * len(X)


def test_moldqn():
    agent = DQNFinalState(Molecule(reward=SklearnReward(FakeEstimator().predict)), MorganFingerprints())
    output = generate_molecules(agent)
    assert 'C' in output  # There is a (0.25)^10 chance that this will fail
