import os
from setuptools import setup, find_packages

setup(
    name='pipeline_cli',
    version='0.0.0',
    packages=find_packages('pipeline_cli'),
    python_requires=">=3.6.*",
    keywords=[
        "funcX",
        "pipeline",
        "HPC",
    ],
    entry_points={'console_scripts':
                  ['pipeline-main=pipeline_cli.main:cli_run',
                   'pipeline-pump=pipeline_cli.pump:cli_run',
                   'pipeline-pull=pipeline_cli.pull:cli_run',
                   ]
                  },
    author="Globus Labs",
    author_email='labs@globus.org',
    license="Apache License, Version 2.0",
    url="https://github.com/funcx-faas/funcx"
)
