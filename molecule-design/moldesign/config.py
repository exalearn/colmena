"""Parsl configuration files"""
import os

from parsl import HighThroughputExecutor, ThreadPoolExecutor
from parsl.addresses import address_by_hostname
from parsl.config import Config
from parsl.launchers import AprunLauncher, SimpleLauncher
from parsl.providers import LocalProvider, CobaltProvider
from parsl.channels import SSHChannel

config = Config(
    executors=[
        HighThroughputExecutor(
            address="localhost",
            label="htex",
            max_workers=2,
            provider=LocalProvider(
                init_blocks=1,
                max_blocks=1
            ),
        ),
        ThreadPoolExecutor(label="local_threads", max_threads=4)
    ],
    strategy=None
)

local_interleaved_config = Config(
    executors=[
        HighThroughputExecutor(
            address="localhost",
            label="psi4",
            max_workers=2,
            provider=LocalProvider(
                init_blocks=1,
                max_blocks=1
            ),
        ),
        HighThroughputExecutor(
            address="localhost",
            label="ml",
            max_workers=1,
            provider=LocalProvider(
                init_blocks=1,
                max_blocks=1
            ),
        ),
        ThreadPoolExecutor(label="local_threads", max_threads=4)
    ],
    strategy=None
)

# Configuration to run on Theta using single-app applications
theta_config = Config(
        executors=[
            HighThroughputExecutor(
                address=address_by_hostname(),
                label="htex",
                max_workers=8,
                provider=LocalProvider(
                    nodes_per_block=os.environ.get("COBALT_JOBSIZE", 1),
                    init_blocks=1,
                    max_blocks=1,
                    launcher=AprunLauncher(overrides='-d 64 --cc depth'),
                    worker_init='''
module load miniconda-3
export PATH=~/software/psi4/bin:$PATH
conda activate /lus/theta-fs0/projects/CSC249ADCD08/colmena/env
export KMP_INIT_AT_FORK=FALSE
export OMP_NUM_THREADS=8
''',
                ),
            ),
            ThreadPoolExecutor(label="local_threads", max_threads=4)
        ],
        strategy=None,
    )

theta_nwchem_config = Config(
    executors=[
        HighThroughputExecutor(
            address=address_by_hostname(),
            label="htex",
            max_workers=int(os.environ.get("COBALT_JOBSIZE", 1)),
            provider=LocalProvider(
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
                launcher=SimpleLauncher(),  # Places worker on the launch node
                worker_init='''
module load miniconda-3
export PATH=~/software/psi4/bin:$PATH
conda activate /lus/theta-fs0/projects/CSC249ADCD08/colmena/env
''',
            ),
        ),
        ThreadPoolExecutor(label="local_threads", max_threads=4)
    ],
    strategy=None,
)

theta_interleaved_config = Config(
    executors=[
        HighThroughputExecutor(
            address=address_by_hostname(),
            label="nwchem",
            max_workers=int(os.environ.get("COBALT_JOBSIZE", 1)) - 1,
            provider=LocalProvider(
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
                launcher=SimpleLauncher(),  # Places worker on the launch node
                worker_init='''
module load miniconda-3
export PATH=~/software/psi4/bin:$PATH
conda activate /lus/theta-fs0/projects/CSC249ADCD08/colmena/env
''',
            ),
        ),
        HighThroughputExecutor(
            address=address_by_hostname(),
            label="single_node",
            max_workers=1,
            provider=LocalProvider(
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
                launcher=AprunLauncher('-d 64 --cc depth'),  # Places worker on the launch node
                worker_init='''
module load miniconda-3
export PATH=~/software/psi4/bin:$PATH
conda activate /lus/theta-fs0/projects/CSC249ADCD08/colmena/env
''',
            ),
        ),
        ThreadPoolExecutor(label="local_threads", max_threads=4)
    ],
    strategy=None,
)

multisite_interleaved_config = Config(
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
            address='localhost', # Using an SSH tunnel
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
