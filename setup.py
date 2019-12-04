import os
from setuptools import setup, find_packages

version_ns = {}
with open(os.path.join("pipeline_prototype", "version.py")) as f:
    exec(f.read(), version_ns)
version = version_ns['VERSION']

with open('requirements.txt') as f:
    install_requires = f.readlines()

setup(
    name='pipeline_prototype',
    version=version,
    packages=find_packages(),
    description='pipeline_prototype: Intelligent Steerable Pipelines on HPC',
    install_requires=install_requires,
    python_requires="==3.6.*",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering"
    ],
    keywords=[
        "funcX",
        "pipeline",
        "HPC",
    ],
    entry_points={'console_scripts':
                  ['pipeline-main=pipeline_prototype.main:cli_run',
                   'pipeline-pump=pipeline_prototype.pump:cli_run',
                   'pipeline-pull=pipeline_prototype.pull:cli_run',
                  ]
    },
    author="Globus Labs",
    author_email='labs@globus.org',
    license="Apache License, Version 2.0",
    url="https://github.com/funcx-faas/funcx"
)
