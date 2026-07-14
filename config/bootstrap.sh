#!/bin/bash
echo "================================================"
echo "POWHEG b2luigi Job Bootstrap"
echo "================================================"
echo "Host: $(hostname -f)"
echo "Start time: $(date)"
echo "Working directory: $(pwd)"
echo "Job ID: ${SLURM_JOB_ID:-local}"
echo "Array Task ID: ${SLURM_ARRAY_TASK_ID:-N/A}"
echo "Process ID: ${SLURM_PROCID:-0}"
echo "================================================"

# Set up environment from job variables
export POWHEG_RUN_DIR="{{POWHEG_RUN_DIR}}"
export POWHEG_LOG_DIR="{{POWHEG_LOG_DIR}}"

# Set up Python path to find the powheg_workflow package
# Add the utilities directory where powheg-luigi-workflow is located
# export PYTHONPATH="/u/jlinder/utilities:/u/jlinder/utilities/powheg-luigi-workflow:${PYTHONPATH}"
# FIXME: Make dynamical to find the correct path to powheg_workflow
source /ptmp/mpp/jlinder/.powrun/bin/activate

# Change to run directory
# cd "${POWHEG_RUN_DIR}" || exit 1

# apptainer shell ./powheg.sif
echo "Run directory: ${POWHEG_RUN_DIR}"
echo "Log directory: ${POWHEG_LOG_DIR}"
echo "================================================"

# Load custom environment variables if present
if [ -f "${POWHEG_RUN_DIR}/.env" ]; then
    echo "Loading custom environment from .env"
    source "${POWHEG_RUN_DIR}/.env"
fi

echo "Bootstrap complete, starting task execution..."
echo "================================================"

