#!/bin/bash

# Store commandline arguments
JOBCODE="$1"
SEED="$2"
PRG="$3"
LOGFILE="$4"

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

echo "[${JOBCODE} (`hostname`)] Running: ${PRG} <<< ${SEED} >> ${LOGFILE} 2>&1"

# ``exec``, and a here-string rather than a pipeline, so that ${PRG} replaces this shell
# instead of running as its child. On an interrupt b2luigi only kills its direct child;
# whatever sits below that is orphaned and has to be killed by hand afterwards. Keeping
# the chain exec-only means the POWHEG process itself is the one that gets the signal.
exec ${PRG} <<< "${SEED}" >> ${LOGFILE} 2>&1
# fi
