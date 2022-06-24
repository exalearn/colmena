# Colmena

[![CI](https://github.com/exalearn/colmena/actions/workflows/CI.yml/badge.svg)](https://github.com/exalearn/colmena/actions/workflows/CI.yml)
[![Documentation Status](https://readthedocs.org/projects/colmena/badge/?version=latest)](https://colmena.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/colmena.svg)](https://badge.fury.io/py/colmena)
[![Coverage Status](https://coveralls.io/repos/github/exalearn/colmena/badge.svg?branch=master)](https://coveralls.io/github/exalearn/colmena?branch=master)

Colmena is a library that supports applications which steer large campaigns of simulations on supercomputers.
Such "high-throughput" searches are commonly deployed on HPC and are, historically, 
guided by humans designating a search space manually &mdash; a time-consuming process.
Colmena was created to explore building applications high-throughput sweeps that replace human steering
with Artificial Intelligence (AI) experimental design methods. 

## Installation

We use Anaconda to define an environments:

``conda env create --file environment.yml --force``

will install all packages needed for the colmena library and demo applications.

Consult our [Installation Guide](https://colmena.readthedocs.io/en/latest/installation.html).

## Using Colmena

We are gradually building ``demo_apps`` which illustrate different approaches to using the prototype.

## Acknowledgements 

This project was supported in part by the Exascale Computing Project (17-SC-20-SC) of the U.S. Department of Energy (DOE) and by DOE’s Advanced Scientific Research Office (ASCR) under contract DE-AC02-06CH11357.
