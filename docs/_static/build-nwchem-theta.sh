#! /bin/bash
# Build script for NWChem, adapted from NWChem complication page's XC40 instructions
#  Added Python support
set -e

export NWCHEM_TOP=$(pwd)
export NWCHEM_TARGET=LINUX64
export USE_MPI=y

export USE_CPPRESERVE=y
export NWCHEM_MODULES="all python"

export USE_MPI=y
export USE_MPIF=y
export USE_MPIF4=y

export USE_OPENMP=y

export INTEL_64ALIGN=1
export USE_NOIO=1
export USE_GAGITHUB=1
export CRAYPE_LINK_TYPE=dynamic
export ARMCI_NETWORK=MPI_TS

export BLAS_SIZE=4
export BLASOPT="  -lmkl_intel_lp64 -lmkl_core -lmkl_intel_thread -lpthread -lm -ldl"
export SCALAPACK=" -lmkl_scalapack_lp64 -lmkl_intel_lp64 -lmkl_core -lmkl_intel_thread -lmkl_blacs_intelmpi_lp64 -lpthread -liomp5  -ldl"
export BLAS_LIB=$BLASOPT
export LAPACK_LIB=$BLASOPT
export SCALAPACK_SIZE=4

export USE_KNL=1
export USE_64TO32=1

export PYTHONHOME=/soft/interpreters/python/2.7/intel/2019.3.075/
export PYTHONVERSION=2.7
export USE_PYTHONCONFIG=Y

export FC=ftn
export CC=cc

# Run the compilation
cd src
make clean
make nwchem_config
make 64_to_32
make -j 6
