from gitinfo import get_git_info
import platform
import requests
import os


def get_platform_info():
    """Get information about the computer running this process"""

    if hasattr(os, 'sched_getaffinity'):
        accessible = len(os.sched_getaffinity(0))
    else:
        accessible = os.cpu_count()
    return {
        'git_commit': get_git_info()['commit'],
        'processor': platform.machine(),
        'python_version': platform.python_version(),
        'python_compiler': platform.python_compiler(),
        'hostname': platform.node(),
        'os': platform.platform(),
        'cpu_name': platform.processor(),
        'n_cores': os.cpu_count(),
        'accessible_cores': accessible,
    }
