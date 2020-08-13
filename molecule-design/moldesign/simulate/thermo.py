import logging

from qcelemental.models import Molecule
from qcelemental.physical_constants import constants
import numpy as np

logger = logging.getLogger(__name__)

c = constants.ureg.Quantity(constants.c, constants.get('c', True).units)
h = constants.ureg.Quantity(constants.h, constants.get('h', True).units)
kb = constants.ureg.Quantity(constants.kb, constants.get('kb', True).units)
r = constants.ureg.Quantity(constants.R, constants.get('R', True).units)


def mass_weighted_hessian(hessian: np.ndarray, molecule: Molecule):
    """Compute the mass-weighted hessian

    Args:
        hessian: Hessian for a molecule
        molecule (Molecule): Molecule used for the Hessian calculation
    Returns:
        Weighted hessian
    """
    # Make a copy of the Hessian
    output = hessian.copy()

    # Apply masses
    masses = molecule.masses
    for i in range(len(masses)):
        for j in range(len(masses)):
            output[3 * i:3 * i + 3, 3 * j:3 * j + 3] /= np.sqrt(masses[i] * masses[j])
    return output


def compute_frequencies(hessian: np.ndarray, molecule: Molecule,
                        units: str = 'hartree / bohr ** 2') -> np.array:
    """Compute the characteristic temperature of vibrational frequencies for a molecule

    Args:
        hessian: Hessian matrix
        molecule: Molecule object
        units: Units for the Hessian matrix
    Returns:
        ([float]): List of vibrational frequencies in Hz
    """

    # Compute the mass-weighted hessian
    mass_hessian = mass_weighted_hessian(hessian, molecule)

    # Compute the eigenvalues and compute frequencies
    eig = np.linalg.eigvals(mass_hessian)
    freq = np.sign(eig) * np.sqrt(np.abs(eig))
    conv = np.sqrt(constants.conversion_factor(f'{units} / amu', 'Hz ** 2'))
    freq *= conv / np.pi / 2  # Converts from angular to ordinary frequency too
    return freq


def compute_zpe(hessian: np.ndarray, molecule: Molecule,
                scaling: float = 1, units: str = 'hartree / bohr ** 2') -> float:
    """Compute the characteristic temperature of vibrational frequencies for a molecule

    Args:
        hessian: Hessian matrix
        molecule: Molecule object
        scaling: How much to scale frequencies before computing ZPE
        units: Units for the Hessian matrix
    Returns:
        (float) Energy for the system in Hartree
    """

    # Get the characteristic temperatures of all vibrations
    freqs = compute_frequencies(hessian, molecule, units)
    freqs = constants.ureg.Quantity(freqs, 'Hz')

    # Scale them
    freqs *= scaling

    # Drop the negative frequencies
    neg_freqs = freqs[freqs < 0]
    if len(neg_freqs) > 0:
        wavenumbers = neg_freqs / c

        # Remove those with a wavenumber less than 80 cm^-1 (basically zero)
        wavenumbers = wavenumbers[wavenumbers.to("1/cm").magnitude < -80]
        if len(wavenumbers) > 0:
            output = ' '.join(f'{x:.2f}' for x in wavenumbers.to("1/cm").magnitude)
            logger.warning(f'{molecule.name} has {len(neg_freqs)} negative components. Largest: [{output}] cm^-1')

    #  Convert the frequencies to characteristic temps
    freqs = constants.ureg.Quantity(freqs, 'Hz')
    temps = (h * freqs / kb)

    # Filter out temperatures less than 300 K (using this as a threshold for negative modes
    temps = temps[np.array(temps.to("K")) > 300.]

    # Compute the ZPE
    zpe = r * temps.sum() / 2
    return zpe.to('hartree').magnitude
