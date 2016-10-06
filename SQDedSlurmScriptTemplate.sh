#!/bin/bash
#SBATCH -N %(nodes)s
#SBATCH --ntasks-per-node %(ncpus)d
#SBATCH -J %(title)s
#SBATCH -p %(queue)s
#SBATCH -t %(walltime)s
#SBATCH --mem %(mem)s
#SBATCH -o SBATCH_%(title)s_out.txt
#SBATCH -e SBATCH_%(title)s_err.txt

%(modload)s

SQDIR="%(logdir)s"
mkdir "$SQDIR"
exec > "$SQDIR/PBS_script.log"
echo "$(date +'%%F %%T') Batch script starting in earnest (pid: $$)."
echo "$(date +'%%F %%T') About to execute %(sqScript)s using task file: %(jobFile)s"

python "%(sqScript)s" \
  --logFile="$SQDIR/SQ.log" \
  --maxTasksPerNode=%(mtpn)s --pnwss --wrapperVerbose \
  "%(jobFile)s"
RETURNCODE=$?
echo "$(date +'%%F %%T') Writing exited file."
touch "$SQDIR/exited"
echo "$(date +'%%F %%T') Batch script exiting, $RETURNCODE."
