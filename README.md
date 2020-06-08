# Colmena

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

Alternatively, you will need to install Python 3.8 on your machine with
all of the packages listed in `requirements.txt`.
Note that we use specific versions of Python packages as we do not yet
view Colmena as "stable" enough to be used in many packages. 
You will also need to install Redis version 5.

## Using Colmena

We are gradually building ``demo_apps`` which illustrate different approaches to using the prototype.

