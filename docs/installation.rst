Installation
============

Colmena is currently in a "research state" and, as a result,
has rather specific installation requirements.

Anaconda
--------

We recommend using Anaconda to install Colmena:

``conda env create --file environment.yml``

Installation with Anaconda insures that you will have the same package
versions we used to develop Colmena, which may help avoid some of the
development difficulties due to the early stages of this package.

Manual Installation + Pip
-------------------------

You will need to install Python 3.8 on your machine with
all of the packages listed in ``requirements.txt``:

``pip install -e .``

Note that we use specific versions of Python packages as we do not yet
view Colmena as "stable" enough to be used in many packages.

You will also need to install Redis version 5.
