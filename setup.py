import os
from setuptools import setup, find_packages

version_ns = {}
with open(os.path.join("colmena", "version.py")) as f:
    exec(f.read(), version_ns)
version = version_ns['VERSION']

with open('requirements.txt') as f:
    install_requires = f.readlines()
    
with open('README.md') as f:
    long_desc = f.read()

setup(
    name='colmena',
    version=version,
    packages=find_packages(),
    include_package_data=True,
    description='colmena: Intelligent Steerable Pipelines on HPC',
    long_description=long_desc,
    long_description_content_type='text/markdown',
    install_requires=install_requires,
    python_requires=">=3.6.*",
    extras_require={
        'funcx': ['funcx>=0.3.4', 'globus_sdk<3']  # TODO (wardlt): Remove Globus SDK version pinning once FuncX supports v3
    },
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
        "parsl",
        "HPC",
    ],
    author="Globus Labs",
    author_email='labs@globus.org',
    license="Apache License, Version 2.0",
    url="https://github.com/exalearn/colmena"
)
