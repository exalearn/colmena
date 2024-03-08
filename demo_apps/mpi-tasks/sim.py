from pathlib import Path

from colmena.models.methods import ExecutableMethod


class Simulation(ExecutableMethod):

    def __init__(self, executable: Path):
        super().__init__(executable=[executable.absolute()],
                         name='simulation',
                         mpi=True,
                         mpi_command_string='mpirun -np {total_ranks}')

    def preprocess(self, run_dir, args, kwargs):
        return [str(args[0])], None

    def postprocess(self, run_dir: Path):
        with open(run_dir / 'colmena.stdout') as fp:
            return float(fp.read().strip())
