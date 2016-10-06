#!/usr/bin/env python

# args TaskFile
import optparse, os, sys
from math import ceil

WALLTIME = 24 * 60 * 60

def dhms2s(s):
    if len(s) == 1:
        return s[0]
    elif len(s) == 2:
        return s[0] * 60 + s[1]
    elif len(s) == 3:
        return s[0] * 3600 + s[1] * 60 + s[2]
    elif len(s) == 4:
        return s[0] * 86400 + s[1] * 3600 + s[2] * 60 + s[3]
    else:
        raise RuntimeError('too many arguments')

def s2int(i):
    try:
        return int(i)
    except ValueError:
        return 0

def gettime(s):
    try:
        return dhms2s([s2int(i) for i in s.split(':')])
    except:
        return 'fas_very_long'

def defqueue(walltime):
    return "general"

# Set cluster dependent values
defaultmempernode = 1024
modload = 'module load Pypkgs/NWS Langs/Python/2.7.11'

opts = optparse.OptionParser(usage='''%prog OPTIONS TaskFile

Generate a Slurm submission script to run a job that works through the
simple queue of tasks given in the TaskFile. Each line of the file
defines a task in the form of a command line to be executed. The job
will distribute the tasks one by one to the nodes allocated by
Slurm, sending a new task to a node when it has completed a
previous one. The job exits when all tasks have completed. Various
logging files are produced that contain information about the
execution of individual tasks and the overall state of the task
processing. These file are located in a subdirectory
"SQ_Files_<Slurm_JOBID>" of the Slurm working directory.
Sibling files of TaskFile, with the suffixes .REMAINING, .ROGUES, .STATUS 
are all created. These contain information about the jobs that had a
suspicious return code, that may not have terminated and an overall
summary, respectively. Finally, Slurm itself will generate an output and
error file in the Slurm working directory.

NOTE: You must submit the generated script to Slurm to actually run the job.

MaxProcsPerNode is forced to be no larger than the number of CPUs per node.
If not specified, MaxProcsPerNode defaults to the number of CPUs per node.''')

opts.add_option('-n', '--nodes', type='int', dest='nodes', default=1,
  help='Number of nodes to use. Not required. Defaults to %default.')
opts.add_option('-p', '--ppn', type='int', dest='ppn', default=20,
  help='Number of CPUs to request per node. Defaults to %default.')
opts.add_option('-t', '--tpn', type='int', dest='tpn',
  help='Maximum number of concurrent tasks per node. Not required. Defaults to ncpus.')
opts.add_option('-m', '--mem', type='int', dest='mem', default=1,
  help='Memory per node. Not required. Defaults to %default')
opts.add_option('-w', '--walltime', dest='walltime', default='1:00:00',
  help='Walltime to request for the Slurm Job in form [[D-]HH:]MM:SS. Not required. Defaults to %default.')
opts.add_option('-q', '--queue', dest='queue', default='general',
  help='Name of queue to use. Not required. Defaults to  %default')
opts.add_option('-N', '--name', dest='name', default='SimpleQueue',
  help='Base job name to use. Not required. Defaults to %default.')
opts.add_option('--logdir', dest='logdir', default='SQ_Files_${SLURM_JOB_ID}',
  help='Name of logging directory. Defaults to %default.')

oArgs, pArgs = opts.parse_args()
if len(pArgs) != 1:
    opts.print_help(sys.stderr)
    sys.exit(1)

# This is eventually passed as an argument to the driver script
jobFile = pArgs[0]

# Set the variables title, nodes, ncpus, queuespec, memspec,
# walltimespec and nodespec, which will be used in the
# following lines from SQDedSlurmScriptTemplate.sh:

#FIX

# Notice that queuespec is a bit sneaky

title = oArgs.name
nodes = oArgs.nodes

ncpus = 20

queue = oArgs.queue

mem = oArgs.mem * 1024 # slurm specifies memory in MB

walltime = oArgs.walltime

logdir = os.path.abspath(oArgs.logdir)

# Compute the "max jobs (or tasks) per node", which is passed as
# an argument to the driver, SQDedDriver.py.  It isn't used for
# settings any PBS directives.
if oArgs.tpn:
    mtpn = min(oArgs.tpn, ncpus)
else:
    mtpn = ncpus

if os.getenv('SQ_PYTHON'):
    pythoninterp = os.getenv('SQ_PYTHON')
else:
    pythoninterp = sys.executable

sq_python_path = os.getenv('SQ_PYTHON_PATH', '')

# Issue a warning if the task file doesn't exist, or if they've
# specified more nodes than makes sense for the number of tasks
# in the task file.
if os.path.exists(jobFile):
    ntasks = len([x for x in open(jobFile).readlines() if x.strip() and x.strip()[0] != '#'])
    maxnodes = int(ceil(ntasks / float(mtpn)))
    if nodes > maxnodes:
        sys.stderr.write('Warning: %s contains %d tasks, which warrants '
                         'at most %d nodes of %d cores, but you requested '
                         '%d nodes\n' % \
                         (jobFile, ntasks, maxnodes, mtpn, nodes))
    else:
        taskspercore = ntasks / float(nodes * mtpn)
        sys.stderr.write('Info: %s contains %d tasks\n' % (jobFile, ntasks))
        sys.stderr.write('Info: average tasks per core: %.1f\n' % taskspercore)
        if nodes > 1 and taskspercore < 1.0:
            sys.stderr.write('Info: you might consider using fewer nodes\n')
else:
    sys.stderr.write('Warning: %s does not currently exist\n' % (jobFile,))

# We assume that related script lives in the same directory as this script.
myDir = os.path.dirname(os.path.realpath(__file__))+os.path.sep
sqScript = myDir + 'SQDedDriver.py'
SlurmScript = open(myDir + 'SQDedSlurmScriptTemplate.sh').read()%locals()

print SlurmScript,
