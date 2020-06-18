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

Eventually, the version of this package will be fixed in the `update_env.yml` definition file. 
We are holding off on that until the development activity on it becomes stable.

## Running

The pipeline is currently defined in `run.py`.

Logan is going to document it later, once I get it working reliably.
