# Synthetic Data Application

A demo Colmena application that generates synthetic tasks which test data bandwidth using different [ProxyStore](https://docs.proxystore.dev/latest/) connectors.
The following options can be configured:

- Task Server type: `--globus` or `--parsl`
- Number of tasks to generate: `--count 10`
- Input size of task: `--input-size [MB]`
- Output size of task: `--output-size [MB]`
- Interval between dispatching tasks: `--interval [seconds]`
- Task sleep duration: `--sleep-time [seconds]`
- Optional ProxyStore backend: `--ps-file`, `--ps-globus`, or `--ps-redis`

## Notes

- Use `python synthetic.py --help` for a full list of arguments.
- Many options will force additional required arguments.
  E.g., `--endpoint [FUNCX-ENDPOINT]` is required if `--funcx` is set.
- The Parsl Task Server by default is configured to run inside of a COBALT
  job on Theta. The `HighThroughputExecutor` config may need to be modified
  depending on your system configuration (e.g., conda env path, launcher,
  etc.). Optionally, `--local` can be specified with `--parsl` to execute
  tasks locally.
