from typing import List

import numpy as np


class Generator:
    """Class that generates new training examples"""

    def __init__(self):
        self.best_x  = 0
        self.best_y = np.inf
        self.width = 2

    def partial_fit(self, x: np.ndarray, y: float):
        """Add another entry"""
        if y < self.best_y:
            self.width = max(abs(self.best_x - x) * 2, 1)  # Mark the increase if needed
            self.best_x = x
            self.best_y = y

    def generate(self, n: int) -> List[float]:
        """Create a set of samples

        Args:
            n (int): Number of entries to create
        Returns:
            ([float]) New samples
        """

        return np.random.normal(self.best_x, self.width, size=(n, 1)).tolist()