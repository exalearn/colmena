from moldesign.simulate import compute_atomization_energy
from qcelemental.models.procedures import QCInputSpecification

qcspec = QCInputSpecification(model=dict(method='scf', basis='3-21g'))


def test_atomization():
    res_with_zpe, opt_data, hess_data = compute_atomization_energy(
        'CC', qcspec, reference_energies={'C': 0, 'H': 0}, code='nwchem'
    )
    assert res_with_zpe < 0
    assert hess_data is not None

    # Run without ZPE
    res_without_zpe, opt_data, hess_data = compute_atomization_energy(
        'CC', qcspec, reference_energies={'C': 0, 'H': 0}, code='nwchem',
        compute_hessian=False
    )
    assert hess_data is None
    assert res_with_zpe > res_without_zpe


def test_restarts():
    compute_atomization_energy(
        'CC', qcspec, reference_energies={'C': 0, 'H': 0}, code='nwchem',
        compute_hessian=True, restart=True
    )
