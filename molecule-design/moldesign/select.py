from typing import List, Callable, Optional
import numpy as np


# TODO (wardlt): Switch to using the active_learning library for these calls. Using simple functions for now


def random_selection(tasks: List[dict], count: int) -> List[dict]:
    """Pick the entries with the highest values of a target function

    Args:
        tasks (dict): List of task descriptions
        count (int): Number of tasks to select
    Returns:
        ([dict]) Selected tasks
    """
    return np.random.choice(tasks, size=(count,), replace=False)


def greedy_selection(tasks: List[dict], count: int,
                     target_fun: Optional[Callable[[dict], float]] = None) -> List[dict]:
    """Pick the that minimize values of a target function

    Args:
        tasks (dict): List of task descriptions
        count (int): Number of tasks to select
        target_fun (callable): Function to determine a rank for a task
    Returns:
        ([dict]) Selected tasks
    """
    prioritized = sorted(tasks, key=target_fun)
    return prioritized[-count:]
