#!/bin/bash

# Runs one branch: pwhg_main for the stages 1-4, or, given an event file, the analysis program
# of the stage lhef or pythia (e.g. lhef_analysis, main-PYTHIA82-lhef).

# Store commandline arguments
JOBCODE="$1"
SEED="$2"
PRG="$3"
LOGFILE="$4"
EVENTFILE="$5"  # Only for the analysis stages

echo "[${JOBCODE} (`hostname`)] Received: SEED=${SEED}, PRG=${PRG}, EVENTFILE=${EVENTFILE}"

# ``exec``, and a here-string rather than a pipeline, so that ${PRG} replaces this shell
# instead of running as its child. On an interrupt b2luigi only kills its direct child;
# whatever sits below that is orphaned and has to be killed by hand afterwards. Keeping
# the chain exec-only means the POWHEG process itself is the one that gets the signal.
if [ -z "${EVENTFILE}" ]; then
    echo "[${JOBCODE} (`hostname`)] Running: ${PRG} <<< ${SEED} >> ${LOGFILE} 2>&1"
    exec ${PRG} <<< "${SEED}" >> ${LOGFILE} 2>&1
elif [ -e "powheg.input" ]; then
    # Only without powheg.input does POWHEG's powheginput read the prefix of its input file.
    echo "[${JOBCODE} (`hostname`)] ERROR: ${PWD}/powheg.input exists, ${PRG} would read it instead of ${SEED}-powheg.input." | tee -a ${LOGFILE} >&2
    exit 1
elif [ ! -f "${SEED}-powheg.input" ]; then
    echo "[${JOBCODE} (`hostname`)] ERROR: No ${SEED}-powheg.input in ${PWD}." | tee -a ${LOGFILE} >&2
    exit 1
else
    # The analysis programs read first the prefix of their input file <seed>-powheg.input,
    # then the name of the event file.
    echo "[${JOBCODE} (`hostname`)] Running: ${PRG} <<< ${SEED}\\n${EVENTFILE} >> ${LOGFILE} 2>&1"
    exec ${PRG} <<< "${SEED}"$'\n'"${EVENTFILE}" >> ${LOGFILE} 2>&1
fi
