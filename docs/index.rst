Colmena: AI-Steering for HPC
============================

Colmena is a Python library for building applications
that combine AI and simulation workflows on HPC.
Its core feature is a communication library that simplifies
tools for intelligently steering large ensemble 
simulations.

Colmena open-source and available on GitHub: https://github.com/exalearn/colmena

What does Colmena do?
---------------------

.. image:: _static/front-page.svg
    :height: 750
    :align: center
    :alt: Colmena high-level sketch

The core concept of Colmena is a "thinking" application. 
The Thinking application is responsible for intelligently 
responding to new data, such as by updating a machine learning 
model or selecting a new simulation with Bayesian optimization.

Colmena provides a few main components to enable building thinking applications:

    #. An extensible base class for building thinking applications with a dataflow-like programming model
    #. A "Task Server" that provides a simplified interface to HPC-ready workflow systems
    #. A high-performance queuing system communicating between tasks server(s) from thinking applications

The `demo applications <https://github.com/exalearn/colmena/tree/master/demo_apps/optimizer-examples>`_
illustrate how to implement different thinking applications that solve optimization problems.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   design
   how-to
   thinker
   task-servers
   source/modules

Why the name "Colmena?"
-----------------------

Colmena is Spanish for "beehive."
Colmena applications, like their namesake,
feature independent agents that perform a variety
of tasks over their lifetimes without complicated
communication between each other.

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
