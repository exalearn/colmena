# Molecular Design Application

This application is an initial version of the electrolyte design application
being built as a collaboration between JCESR and ExaLearn. 
Our goal is to be as close to the final application as possible 
without making installation on different HPC systems burdensome.

## Installation

The additional packages needed by this application are defined in `update_env.yml`

Install them by activating the colmena environment environment and then calling:
`conda env update --file update_env.yml`

Then clone the `covid-drug-design repository <https://github.com/exalearn/covid-drug-design>`_
 to install the `molgym` package:

.. code-block: bash
   
    git clone git@github.com:exalearn/covid-drug-design.git
    pip install -e .

Eventually, the version of `molgym` will be fixed in the `update_env.yml` definition file. 
We are holding off on that until the development activity on `molgym` slows down.

You should also install NWChem (available as an Ubuntu package) on your system or,
otherwise, change the default code in `moldesign/simulate.py` to `psi4`.

## Running

There are currently two demo apps for using Colmena for molecular design: `local-greey` and `interleaved`.
Each are available in subdirectories of this folder and documentation for running them is available in 
those folders.

