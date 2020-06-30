# Colmena

[![Build Status](https://travis-ci.com/exalearn/colmena.svg?branch=master)](https://travis-ci.com/exalearn/colmena)
[![Documentation Status](https://readthedocs.org/projects/colmena/badge/?version=latest)](https://colmena.readthedocs.io/en/latest/?badge=latest)

Colmena is a library that supports applications which steer large campaigns of simulations on supercomputers.
Such "high-throughput" searches are commonly deployed on HPC and are, historically, 
guided by humans designating a search space manually &mdash; a time-consuming process.
Colmena was created to explore building applications high-throughput sweeps that replace human steering
with Artificial Intelligence (AI) experimental design methods. 

## Installation

We recommend using Anaconda to install Colmena:

`conda env create --file environment.yml`

Installation with Anaconda insures that you will have the same package
versions we used to develop Colemna, which may help avoid some of the 
development difficulties due to the early stages of this package.

Alternatively, you will need to install Python 3.7 on your machine with
all of the packages listed in `requirements.txt`.
Note that we use specific versions of Python packages as we do not yet
view Colmena as "stable" enough to be used in many packages. 
You will also need to install Redis version 5.

### Full Installation

Install the base environment needed for Colmena as well as the molecular design app available
from this repository with conda: 

```bash
conda env create --file full_environment.yml --force
```

Note that the above command will delete any previous installation of the Colmena environment.

----
**WARNING**: Tensorflow is a problem when building this environment. If you use OSX, you 
will need to move tensorflow in `full_environment.yml` to the list of pip requirements at 
the end of the list of dependencies. If you want to use a GPU, replace `tensorflow` with 
`tensorflow-gpu` in the environment specification so that Anaconda will install
the required CUDA libraries.

----

----
**WARNING**: OSX users will need to install Redis via brew:

```bash
brew update
brew install redis
brew services start redis
```

----

## Using Colmena

We are gradually building ``demo_apps`` which illustrate different approaches to using the prototype.

