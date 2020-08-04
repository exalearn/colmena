Installation
============

Colmena is currently in a "research state" and, as a result,
has rather specific installation requirements.
We use Anaconda to specify two different environments:

- ``environment.yml``: The basic environment needed to run Colmena, but not the demo applications.
- ``full_environment.yml``: Requirements for Conda and the molecular design applications used as an example.

Install them by calling: ``conda env create --file full_environment.yml --force``

The installation will install the ``colmena`` and its libraries in development mode
so that any changes you make to them are automatically available to the environment.

The full environment will install two computational chemistry codes,
Psi4 and XTB, that can be used in our applications.
You may also want to install NWChem, which is available as an Ubuntu package
and can be installed on OSX.

System-Specific Instructions
----------------------------

Tensorflow and Redis, in particular, lead to special requirements for some systems.

Systems with GPUs
+++++++++++++++++

Edit the ``tensorflow==2.*`` requirement in the environment to ``tensorflow-gpu==2.*``
to install the CUDA-enabled version of Tensorflow.

OSX
+++

The latest Tensorflow binaries for OSX are available through PyPI instead of Anaconda.
Move the ``tesnorflow`` line to the ``- pip:`` block at the end of the environment file.

You will also need to install redis manually.

Windows
+++++++

Sorry, we do not support Windows at this time and recommend you install the Windows subsystem for Linux.
We have experienced some issues around Parsl not being able to reserve ports on WSL, but have not figured it out yet.


Supercomputers
++++++++++++++

Python environments and availability of Quantum Chemistry applications vary dramatically
across different supercomputers.
Here, we describe the Colmena installation process for different supercomptuers.

Theta (Cray XC40 at ALCF)
~~~~~~~~~~~~~~~~~~~~~~~~~

You can use the environment file without modification when installing Colmena on Theta.
We do recommend you install the environment files to the Lustre file system rather than your
home directory.
For example, ``conda env create --file full_environment.yml -p ./env`` will install the environment
files to a folder you specify rather than your home directory.

We recommend the following procedure:

.. code-block:: bash

    # Clone colmena
    git clone https://github.com/exalearn/colmena.git
    cd colmena

    # Build the anaconda environment
    module load miniconda-3/latest  # Loads the ALCF-provided miniconda
    conda deactivate  # Avoids permissions issues around editing the base environment
    conda env create --file full_environment.yml -p ./env
    conda activate ./env

Our applications on Theta require a build of NWChem with Python scripting enabled.
The version referred to in the Theta queue scripts for each application point to the proper location
in the CSC249ADCD08 filesystem.
Contact Logan Ward for access or, if you would like to build it on your own,
download the `NWChem source code from GitHub
<https://github.com/nwchemgit/nwchem/releases/download/6.8.1-release/
nwchem-6.8.1-release.revision-v6.8-133-ge032219-src.2018-06-14.tar.bz2>`_
and use `our build script <_static/build-nwchem-theta.sh>`_.
