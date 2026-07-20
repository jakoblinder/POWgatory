#!/bin/bash

# Store commandline arguments
JOBCODE="$1"
SEED="$2"
PRG="$3"

echo "[${JOBCODE} (`hostname`)] Received: SEED=${SEED}, PRG=${PRG}"

# if [[ "$PRG" =~ "lhef" ]] || [[ "$PRG" =~ "PYTHIA" ]]; then
#     if [ -f "${JOBCODE}-powheg.input" ]; then
#         echo "[${JOBCODE} (`hostname`)] Running: ${PRG} < input-${SEED} 2>&1"
#         ${PRG} < input-${SEED} 2>&1
#     else
#         echo "[${JOBCODE} (`hostname`)] WARNING: No ${JOBCODE}-powheg.input file. Seed is skipped."
#         sleep 1s
#     fi
# else

echo "[${JOBCODE} (`hostname`)] Running: echo ${SEED} | ${PRG} 2>&1"
echo ${SEED} | ${PRG} 2>&1
# fi
