#!/bin/bash
#BSUB -J %(title)s
#BSUB -q %(queue)s
#BSUB -n %(procs)d
#BSUB -R "span[%(spanspec)s]"
#BSUB -W %(walltime)s
#BSUB -M %(mem)s
#BSUB -oo LSF_%(title)s_out.txt

%(modload)s
SQDIR="%(logdir)s"
mkdir "$SQDIR"
exec > "$SQDIR/LSF_script.log"
echo "$(date +'%%F %%T') Batch script starting in earnest (pid: $$)."
echo "$(date +'%%F %%T') About to execute %(sqScript)s using task file: %(jobFile)s"
PYTHON_BIN="$SQ_PYTHON_DIR/bin"
PYTHON_LIB="$SQ_PYTHON_DIR/lib"
if [ -z "$LD_LIBRARY_PATH" ]; then
  export LD_LIBRARY_PATH="$PYTHON_LIB"
else
  export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$PYTHON_LIB"
fi
if [ -z "$PYTHONPATH" ]; then
  export PYTHONPATH="$SQ_NWS_DIR"
else
  export PYTHONPATH="$SQ_NWS_DIR:$PYTHONPATH"
fi
"$PYTHON_BIN/python" "%(sqScript)s" \
  --logFile="$SQDIR/SQ.log" \
  --maxTasksPerNode=%(mtpn)s --pnwss --wrapperVerbose \
  "%(jobFile)s"
RETURNCODE=$?
echo "$(date +'%%F %%T') Writing exited file."
touch "$SQDIR/exited"
echo "$(date +'%%F %%T') Batch script exiting, $RETURNCODE."
