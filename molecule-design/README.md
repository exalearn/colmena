# Molecular Design Application

This application is an initial version of the electrolyte design application
being built as a collaboration between JCESR and ExaLearn. 
Our goal is to be as close to the final application as possible 
without making installation on different HPC systems burdensome.

## Installation

The additional packages needed by this application are defined in `update_env.yml`

Install them by activitating the pipeline environment and then calling:
`conda env update --file update_env.yml`

Note that we currently require a [bug fix from Parsl](https://github.com/Parsl/parsl/pull/1620)
that has yet to be published to PyPI. Given it only changes 2 lines of Parsl, I would recommend editing your
installed Parsl version rather than cloning a development version.
\[Hint: use `pip show parsl` to find where your Parsl is installed.\]

## Running

The pipeline is currently defined in `run.py`.

Logan is going to document it later, once I get it working reliably.
