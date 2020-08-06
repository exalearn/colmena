#! /bin/bash
# Script for running the code with the XTB components, useful for debugging/dev work

python run.py --mpnn-directory ./notebooks/nwchem-validation \
    --initial-agent ./notebooks/nwchem-validation/moldqn-training/agent.pkl \
    --initial-search-space ./notebooks/nwchem-validation/moldqn-training/best_mols.json \
    --initial-database ./notebooks/nwchem-validation/initial_database.json \
    --reference-energies ./notebooks/nwchem-validation/ref_energies.json \
    --qc-spec ./notebooks/nwchem-validation/qc_config.json \
    $@
