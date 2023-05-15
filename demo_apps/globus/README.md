# Globus Compute Task Server Demo

This demo recreates the "streaming" optimization application using the Globus Compute server.
Globus Compute allows the calculations to run on remote resources without any network configuration changes.

## Setup

Using Globus Compute requires a few additional steps beyond what is required for a standard Colmena application.

First, install and start a [Globus Compute endpoint](https://funcx.readthedocs.io/en/latest/endpoints.html) on the system that you would like to run. 
Make sure to install the Colmena python library in the Python environment used by your endpoint (e.g., by calling `pip install colmena` in the environment in which you are installing Globus Compute).
Record the endpoint ID given to you when you install the Globus Compute endpoint. 

Colmena also requires additional libraries to run Globus Compute. When installing Colmena, add them using PyPi's extra dependencies mechanism: `pip install colmena[globus]`.

## Running the Example

The example requires you to specify the endpoint on which tasks will run.
Provide it as the only requirement of the function, which will look something like:

```commandline
python run.py c845e24a-154e-4340-abc1-f83948d9454b
```

 