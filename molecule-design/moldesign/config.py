"""Functions to generate configurations"""
import os

from parsl import HighThroughputExecutor, ThreadPoolExecutor
from parsl.addresses import address_by_hostname
from parsl.config import Config
from parsl.launchers import AprunLauncher, SimpleLauncher
from parsl.providers import LocalProvider, CobaltProvider
from parsl.channels import SSHChannel
from parsl.monitoring import MonitoringHub


def simple_config(n_workers: int, log_dir: str) -> Config:
    """Single type of worker. All running on local system

    Args:
        n_workers (int): Number of parallel workers
        log_dir: Path to store monitoring DB and parsl logs
    Returns:
        (Config): Colmena-ready configuration
    """
    return Config(
        executors=[
            HighThroughputExecutor(
                address="localhost",
                label="htex",
                max_workers=n_workers,
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1
                ),
            ),
            ThreadPoolExecutor(label="local_threads", max_threads=4)
        ],
        run_dir=log_dir,
        strategy=None
    )


def local_interleaved_config(qc_workers: int, ml_workers: int, log_dir: str) -> Config:
    """All workers on the local machine, split between QC and ML tasks

    Args:
        qc_workers: Number of quantum chemistry workers
        ml_workers: Number of machine learning workers
        log_dir: Path to store monitoring DB and parsl logs
    Returns:
        (Config): Desired configuration
    """
    return Config(
        executors=[
            HighThroughputExecutor(
                address="localhost",
                label="qc",
                max_workers=qc_workers,
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1
                ),
            ),
            HighThroughputExecutor(
                address="localhost",
                label="ml",
                max_workers=ml_workers,
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1
                ),
            ),
            ThreadPoolExecutor(label="local_threads", max_threads=4)
        ],
        run_dir=log_dir,
        strategy=None
    )


def theta_nwchem_config(ml_workers: int, log_dir: str, nodes_per_nwchem: int = 2,
                        total_nodes: int = int(os.environ.get("COBALT_JOBSIZE", 1))) -> Config:
    """Theta configuration where QC workers sit on the launch node (to be able to aprun)
    and ML workers are placed on compute nodes

    Args:
        ml_workers: Number of nodes dedicated to ML tasks
        nodes_per_nwchem: Number of nodes per NWChem computation
        log_dir: Path to store monitoring DB and parsl logs
        total_nodes: Total number of nodes available. Default: COBALT_JOBSIZE
    Returns:
        (Config) Parsl configuration
    """
    nwc_nodes = total_nodes - ml_workers
    assert nwc_nodes % nodes_per_nwchem == 0, "NWChem node count not a multiple of nodes per task"
    nwc_workers = nodes_per_nwchem // nwc_nodes

    return Config(
        executors=[
            HighThroughputExecutor(
                address=address_by_hostname(),
                label="qc",
                max_workers=nwc_workers,
                provider=LocalProvider(
                    nodes_per_block=1,
                    init_blocks=1,
                    max_blocks=1,
                    launcher=SimpleLauncher(),  # Places worker on the launch node
                    worker_init='''
module load miniconda-3
conda activate /lus/theta-fs0/projects/CSC249ADCD08/colmena/env
''',
                ),
            ),
            HighThroughputExecutor(
                address=address_by_hostname(),
                label="ml",
                max_workers=1,
                provider=LocalProvider(
                    nodes_per_block=ml_workers,
                    init_blocks=1,
                    max_blocks=1,
                    launcher=AprunLauncher(overrides='-d 64 --cc depth'),  # Places worker on the launch node
                    worker_init='''
module load miniconda-3
conda activate /lus/theta-fs0/projects/CSC249ADCD08/colmena/env
    ''',
                ),
            ),
            ThreadPoolExecutor(label="local_threads", max_threads=4)
        ],
        monitoring=MonitoringHub(
            hub_address=address_by_hostname(),
            monitoring_debug=False,
            resource_monitoring_interval=10,
            logdir=log_dir,
            logging_endpoint=f'sqlite:///{os.path.join(log_dir, "monitoring.db")}'
        ),
        run_dir=log_dir,
        strategy=None,
    )


def theta_xtb_config(ml_workers: int, log_dir: str, xtb_per_node: int = 1,
                     total_nodes: int = int(os.environ.get("COBALT_JOBSIZE", 1))):
    """Theta configuration where QC tasks and ML tasks run on separate nodes.
    Designed to support XTB computations, which are single-node.

    There are no MPI tasks in this configuration.

    Args:
        ml_workers: Number of nodes dedicated to ML tasks
        xtb_per_node: Number of XTB calculations
        log_dir: Path to store monitoring DB and parsl logs
        total_nodes: Total number of nodes available. Default: COBALT_JOBSIZE
    Returns:
        (Config) Parsl configuration
    """
    xtb_nodes = total_nodes - ml_workers

    return Config(
        executors=[
            HighThroughputExecutor(
                address=address_by_hostname(),
                label="qc",
                max_workers=xtb_per_node,
                provider=LocalProvider(
                    nodes_per_block=xtb_nodes,
                    init_blocks=1,
                    max_blocks=1,
                    launcher=AprunLauncher(overrides='-d 64 --cc depth'),  # Places worker on the launch node
                    worker_init='''
module load miniconda-3
conda activate /lus/theta-fs0/projects/CSC249ADCD08/colmena/env
''',
                ),
            ),
            HighThroughputExecutor(
                address=address_by_hostname(),
                label="ml",
                max_workers=1,
                provider=LocalProvider(
                    nodes_per_block=ml_workers,
                    init_blocks=1,
                    max_blocks=1,
                    launcher=AprunLauncher(overrides='-d 64 --cc depth'),  # Places worker on the launch node
                    worker_init='''
module load miniconda-3
conda activate /lus/theta-fs0/projects/CSC249ADCD08/colmena/env
''',
                ),
            ),
            ThreadPoolExecutor(label="local_threads", max_threads=4)
        ],
        monitoring=MonitoringHub(
            hub_address=address_by_hostname(),
            hub_port=55055,
            monitoring_debug=False,
            resource_monitoring_interval=10,
            logdir=log_dir,
            logging_endpoint=f'sqlite:///{os.path.join(log_dir, "monitoring.db")}'
        ),
        run_dir=log_dir,
        strategy=None,
    )


def multisite_nwchem_config() -> Config:
    """Experimental multi-site configuration"""
    return Config(
        retries=1,
        executors=[
            HighThroughputExecutor(
                address=address_by_hostname(),
                label="qc",
                max_workers=8,  # One task per node
                provider=CobaltProvider(
                    cmd_timeout=120,
                    nodes_per_block=8,
                    account='CSC249ADCD08',
                    queue='debug-cache-quad',
                    walltime="1:00:00",
                    init_blocks=1,
                    max_blocks=1,
                    launcher=SimpleLauncher(),  # Places worker on the launch node
                    scheduler_options='#COBALT --attrs enable_ssh=1',
                    worker_init='''
module load miniconda-3
export PATH=~/software/psi4/bin:$PATH
conda activate /lus/theta-fs0/projects/CSC249ADCD08/colmena/env

# NWChem settings
export PATH="/home/lward/software/nwchem-6.8.1/bin/LINUX64:$PATH"
module load atp
export MPICH_GNI_MAX_EAGER_MSG_SIZE=16384
export MPICH_GNI_MAX_VSHORT_MSG_SIZE=10000
export MPICH_GNI_MAX_EAGER_MSG_SIZE=131072
export MPICH_GNI_NUM_BUFS=300
export MPICH_GNI_NDREG_MAXSIZE=16777216
export MPICH_GNI_MBOX_PLACEMENT=nic
export MPICH_GNI_LMT_PATH=disabled
export COMEX_MAX_NB_OUTSTANDING=6
export LD_LIBRARY_PATH=/opt/intel/compilers_and_libraries_2018.0.128/linux/compiler/lib/intel64_lin:$LD_LIBRARY_PATH
''',
                ),
            ),
            HighThroughputExecutor(
                address='localhost',  # Using an SSH tunnel
                worker_ports=(54382, 54008),
                label="ml",
                max_workers=1,
                working_dir='/homes/lward/parsl',
                worker_logdir_root='/homes/lward/parsl',
                provider=LocalProvider(
                    channel=SSHChannel('lambda5.cels.anl.gov', script_dir='/home/lward/parsl'),
                    nodes_per_block=1,
                    init_blocks=1,
                    max_blocks=1,
                    launcher=SimpleLauncher(),
                    worker_init='''
source /homes/lward/miniconda3/etc/profile.d/conda.sh
conda activate colmena_full
export CUDA_VISIBLE_DEVICES=17  # Pins to a GPU worker
''',
                ),
            ),
            ThreadPoolExecutor(label="local_threads", max_threads=4)
        ],
        strategy=None,
    )
