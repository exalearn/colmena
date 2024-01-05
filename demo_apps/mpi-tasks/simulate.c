#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char** argv) {
    // Check to make sure the
    if (argc != 2) {
        printf("Usage: %s <input value>\n", argv[0]);
        return 1;
    }

    // Get the user's input value
    float x = atof(argv[1]);

    // Perform the computation
    float y = (2 - x) * (x - 2);

    // Initialize the MPI environment
    MPI_Init(NULL, NULL);

    // Get the rank of the process
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    // If we are the root rank, print the value
    if (world_rank == 0) {
        printf("%.8e\n", y);
    }

    // Finalize the MPI environment.
    MPI_Finalize();
}
