from moldesign.simulate import compute_atomization_energy, compute_reference_energy
from qcelemental.models.procedures import QCInputSpecification

qcspec = QCInputSpecification(driver='gradient', model=dict(method='hf', basis='6-31g'))


def test_simple_mol():
    assert compute_atomization_energy('C', qcspec, {'C': -2, 'H': -3}) < 0


def test_ref_energy():
    assert compute_reference_energy('C', qcspec) < 0
