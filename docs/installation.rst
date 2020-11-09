Installation
============

Colmena is currently in a "research state" and, as a result,
has rather specific installation requirements.
We therefore recommend using Anaconda to build a specific environment:

``conda env create --file environment.yml --force``

The installation will install the ``colmena`` and its libraries in development mode
so that any changes you make to them are automatically available to the environment.

Installation on Windows
-----------------------

Colmena requires Redis to run, which is only available on Linux and OSX via Anaconda
for the recent versions.
We recommend installing the Windows Subsystem for Linux in order to use Colmena from a Windows system.

