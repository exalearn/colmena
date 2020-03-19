"""Utilities for scoring molecules based on their properties"""

from typing import List

from sklearn.base import BaseEstimator


def add_score(model: BaseEstimator, designs: List[str]) -> List[float]:
    """Assign a score to a series of designs given a machine learning model

    Args:
        model (BaseEstimator): Scikit-learn model
        designs ([str]): List of strings describing the model
    """

    # Run inference
    y_pred = model.predict(designs)

    # Return results
    return y_pred.to_list()
