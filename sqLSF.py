#!/usr/bin/env python

# args TaskFile
import optparse, os, sys

NORMAL_WALLTIME = 24 * 60

def hm2m(s):
    if len(s) == 1:
        return s[0]
    elif len(s) == 2:
        return s[0] * 60 + s[1]
    else:
        raise RuntimeError('too many arguments')

def s2int(i):
    try:
        return int(i)
    except ValueError:
        return 0

def gettime(s):
    try:
        return hm2m([s2int(i) for i in s.split(':')])
    except:
        return 60

def defqueue(walltime):
    normal_walltime = NORMAL_WALLTIME
    s = gettime(walltime)
    if s <= normal_walltime:
        queue = 'shared'
    else:
        queue = 'long'
    return queue

# Set cluster dependent values
sq_python_dir = os.environ.get('SQ_PYTHON_DIR')
sq_nws_dir = os.environ.get('SQ_NWS_DIR')
modload = 'module load Langs/GCC/4.8.2 MPI/OpenMPI/1.8.1\nexport SQ_PYTHON_DIR="%s"\nexport SQ_NWS_DIR="%s"\n' % (sq_python_dir, sq_nws_dir)

opts = optparse.OptionParser(usage='''%prog OPTIONS TaskFile

Generate an LSF submission script to run a job that works through the
simple queue of tasks given in the TaskFile. Each line of the file
defines a task in the form of a command line to be executed. The job
will distribute the tasks one by one to the processors allocated by
LSF, sending a new task to a node when it has completed a
previous one. The job exits when all tasks have completed. Various
logging files are produced that contain information about the
execution of individual tasks and the overall state of the task
processing. These file are located in a subdirectory
"SQ_Files_<LSB_JOBID>" of the working directory.
Sibling files of TaskFile, with the suffixes .REMAINING, .ROGUES, .STATUS 
are all created. These contain information about the jobs that had a
suspicious return code, that may not have terminated and an overall
summary, respectively. Finally, LSF itself will generate an output and
error file in the working directory.

NOTE: You must submit the generated script to LSF to actually run the job.''')

opts.add_option('-n', type='int', dest='procs', default=10,
  help='Number of processors to use. Not required. Defaults to %default.')
opts.add_option('-p', '--ptile', type='int', dest='ptile',
  help='Number of processors to request per node.')
opts.add_option('-t', '--tpn', type='int', dest='tpn',
  help='Maximum number of concurrent tasks per node. Not required.')
opts.add_option('-W', '--walltime', dest='walltime', default='1:00',
  help='Walltime to request for the LSF Job in form HH:MM. Not required. Defaults to %default.')
opts.add_option('-q', '--queue', dest='queue',
  help='Name of queue to use. Not required. Default value is computed based on the walltime limit.')
opts.add_option('-J', '--name', dest='name', default='SimpleQueue',
  help='Base job name to use. Not required. Defaults to %default.')
opts.add_option('--logdir', dest='logdir', default='SQ_Files_${LSB_JOBID}',
  help='Base job name to use. Not required. Defaults to %default.')

oArgs, pArgs = opts.parse_args()
if len(pArgs) != 1:
    opts.print_help(sys.stderr)
    sys.exit(1)

# This is eventually passed as an argument to the driver script
jobFile = pArgs[0]

# Set the variables title, procs, queue, spanspec, and walltime, which
# will be used in the following lines from SQDedLSFScriptTemplate.sh:

  #BSUB -J %(title)s
  #BSUB -q %(queue)s
  #BSUB -n %(procs)d
  #BSUB -R "span[%(spanspec)s]"
  #BSUB -W %(walltime)s
  #BSUB -oo LSF_%(title)s_out.txt

title = oArgs.name
procs = oArgs.procs

walltime = oArgs.walltime
walltimev = walltime.split(":")
if len(walltimev) > 2:
    sys.stderr.write("Error: walltime should have the form: HH:MM\n")
    sys.exit(1)

if oArgs.queue:
    queue = oArgs.queue
else:
    queue = defqueue(walltime)

logdir = os.path.abspath(oArgs.logdir)

# Compute the "max jobs (or tasks) per node", which is passed as
# an argument to the driver, SQDedDriver.py.  It isn't used for
# settings any BSUB directives.
if oArgs.tpn:
    mtpn = oArgs.tpn
else:
    mtpn = 1000000

if oArgs.ptile:
    spanspec = "ptile=%d" % oArgs.ptile
else:
    spanspec = ""

if os.getenv('SQ_PYTHON'):
    pythoninterp = os.getenv('SQ_PYTHON')
else:
    pythoninterp = sys.executable

sq_python_path = os.getenv('SQ_PYTHON_PATH', '')

# Issue a warning if the task file doesn't exist, or if they've
# specified more processors than makes sense for the number of tasks
# in the task file.
if os.path.exists(jobFile):
    ntasks = len([x for x in open(jobFile).readlines() if x.strip() and x.strip()[0] != '#'])
    if procs > ntasks:
        sys.stderr.write('Warning: %s contains %d tasks, which warrants '
                         'at most %d processors, but you requested '
                         '%d processors\n' % \
                         (jobFile, ntasks, ntasks, procs))
    else:
        taskspercore = ntasks / float(procs)
        sys.stderr.write('Info: %s contains %d tasks\n' % (jobFile, ntasks))
        sys.stderr.write('Info: average tasks per core: %.1f\n' % taskspercore)
else:
    sys.stderr.write('Warning: %s does not currently exist\n' % (jobFile,))

# We assume that related script lives in the same directory as this script.
myDir = os.path.dirname(os.path.realpath(__file__))+os.path.sep
sqScript = myDir + 'SQDedDriver.py'
LSFScript = open(myDir + 'SQDedLSFScriptTemplate.sh').read()%locals()

print LSFScript,
