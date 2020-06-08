import os
from setuptools import setup, find_packages

version_ns = {}
with open(os.path.join("colmena", "version.py")) as f:
    exec(f.read(), version_ns)
version = version_ns['VERSION']

with open('requirements.txt') as f:
    install_requires = f.readlines()

setup(
    name='colmena',
    version=version,
    packages=find_packages(),
    include_package_data=True,
    description='colmena: Intelligent Steerable Pipelines on HPC',
    install_requires=install_requires,
    python_requires=">=3.6.*",
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
                  ['colmena-main=colmena.main:cli_run',
                   'colmena-pump=colmena.pump:cli_run',
                   'colmena-pull=colmena.pull:cli_run']
                  },
    author="Globus Labs",
    author_email='labs@globus.org',
    license="Apache License, Version 2.0",
    url="https://github.com/funcx-faas/funcx"
)
