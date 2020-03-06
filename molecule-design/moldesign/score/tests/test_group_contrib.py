from moldesign.score.group_contrib import GroupFeaturizer
import numpy as np


def test_featurizer():
    feat = GroupFeaturizer()
    feat.fit(['C'])
    assert np.isclose(feat.transform(['C']), [1]).all()
    assert np.isclose(feat.transform(['F']), [0]).all()
