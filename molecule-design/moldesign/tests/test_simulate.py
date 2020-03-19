import os

from moldesign.simulate import compute_atomization_energy
from qcelemental.models.procedures import QCInputSpecification

qcspec = QCInputSpecification(driver='gradient', model=dict(method='b3lyp', basis='6-31g'))


def test_simple_mol():
    print(compute_atomization_energy('C', qcspec, {}))
