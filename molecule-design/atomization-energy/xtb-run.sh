#! /bin/bash
# Script for running the code with the XTB components, useful for debugging/dev work

python run.py --mpnn-directory ./notebooks/xtb \
    --initial-agent ./notebooks/xtb/moldqn-training/agent.pkl \
    --initial-search-space ./notebooks/xtb/moldqn-training/best_mols.json \
    --initial-database ./notebooks/xtb/initial_database.json \
    --reference-energies ./notebooks/xtb/ref_energies.json \
    --qc-spec ./notebooks/xtb/qc_config.json \
    $@
