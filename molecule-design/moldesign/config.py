"""Parsl configuration files"""
import os

from parsl import HighThroughputExecutor, ThreadPoolExecutor
from parsl.addresses import address_by_hostname
from parsl.config import Config
from parsl.launchers import AprunLauncher, SimpleLauncher
from parsl.providers import LocalProvider

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
