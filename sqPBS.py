#!/usr/bin/env python

# args TaskFile
import optparse, os, sys
from math import ceil

NORMAL_WALLTIME = 24 * 60 * 60
LONG_WALLTIME = 3 * 24 * 60 * 60
K_NORMAL_WALLTIME = 7 * 24 * 60 * 60
K_LONG_WALLTIME = 7 * 24 * 60 * 60

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
    domain = os.environ.get('DOMAIN')
    if domain == 'bulldogk':
        normal_walltime = K_NORMAL_WALLTIME
        long_walltime = K_LONG_WALLTIME
    else:
        normal_walltime = NORMAL_WALLTIME
        long_walltime = LONG_WALLTIME
    s = gettime(walltime)
    if s <= normal_walltime:
        queue = 'fas_normal'
    elif s <= long_walltime:
        queue = 'fas_long'
    else:
        queue = 'fas_very_long'
    return queue

# Set cluster dependent values
domain = os.environ.get('DOMAIN')
sq_python_dir = os.environ.get('SQ_PYTHON_DIR')
sq_nws_dir = os.environ.get('SQ_NWS_DIR')
defaultmempernode = 35
modload = 'module unload MPI Langs Compilers\nmodule load MPI/OpenMPI/1.4.4\nexport SQ_PYTHON_DIR="%s"\nexport SQ_NWS_DIR="%s"\n' % (sq_python_dir, sq_nws_dir)

opts = optparse.OptionParser(usage='''%prog OPTIONS TaskFile

Generate a PBS submission script to run a job that works through the
simple queue of tasks given in the TaskFile. Each line of the file
defines a task in the form of a command line to be executed. The job
will distribute the tasks one by one to the nodes allocated by
PBS, sending a new task to a node when it has completed a
previous one. The job exits when all tasks have completed. Various
logging files are produced that contain information about the
execution of individual tasks and the overall state of the task
processing. These file are located in a subdirectory
"SQ_Files_<PBS_JOBID>" of the PBS working directory (PBS_O_WORKDIR).
Sibling files of TaskFile, with the suffixes .REMAINING, .ROGUES, .STATUS 
are all created. These contain information about the jobs that had a
suspicious return code, that may not have terminated and an overall
summary, respectively. Finally, PBS itself will generate an output and
error file in the PBS working directory.

NOTE: You must submit the generated script to PBS to actually run the job.

MaxProcsPerNode is forced to be no larger than the number of CPUs per node.
If not specified, MaxProcsPerNode defaults to the number of CPUs per node.''')

opts.add_option('-n', '--nodes', type='int', dest='nodes', default=1,
  help='Number of nodes to use. Not required. Defaults to %default.')
opts.add_option('-p', '--ppn', type='int', dest='ppn', default=8,
  help='Number of CPUs to request per node. Deprecated. Forced to 8.')
opts.add_option('-t', '--tpn', type='int', dest='tpn',
  help='Maximum number of concurrent tasks per node. Not required. Defaults to ncpus.')
opts.add_option('-m', '--mem', type='int', dest='mem',
  help='Per-node memory in GB to request for entire job. Not required. '
       'Defaults to (nodes * %d GB).' % defaultmempernode)
opts.add_option('-a', '--accesspolicy', type='choice', dest='accesspolicy',
  choices=('shared', 'singlejob', 'singletask', 'singleuser', 'uniqueuser'),
  help='Specify the Moab node access policy. Not required. Forced to singlejob if 8 CPUs are requested.  No default otherwise.')
opts.add_option('-w', '--walltime', dest='walltime', default='1:00:00',
  help='Walltime to request for the PBS Job in form HH:MM:SS. Not required. Defaults to %default.')
opts.add_option('-q', '--queue', dest='queue',
  help='Name of queue to use. Not required. Default value is computed based on the walltime limit.')
opts.add_option('-N', '--name', dest='name', default='SimpleQueue',
  help='Base job name to use. Not required. Defaults to %default.')
opts.add_option('--logdir', dest='logdir', default='SQ_Files_${PBS_JOBID}',
  help='Base job name to use. Not required. Defaults to %default.')
opts.add_option('--useprocs', dest='useprocs', default=False,
  action='store_true',
  help='Should procs/tpn be used rather than nodes/ppn. Defaults to %default.')

oArgs, pArgs = opts.parse_args()
if len(pArgs) != 1:
    opts.print_help(sys.stderr)
    sys.exit(1)

# This is eventually passed as an argument to the driver script
jobFile = pArgs[0]

# Set the variables title, nodes, ncpus, queuespec, memspec,
# walltimespec and nodespec, which will be used in the
# following lines from SQDedPBSScriptTemplate.sh:

  #PBS -N %(title)s%(queuespec)s
  #PBS -l %(nodespec)s%(memspec)s%(walltimespec)s
  #PBS -o PBS_%(title)s_out.txt
  #PBS -e PBS_%(title)s_err.txt

# Notice that queuespec is a bit sneaky

title = oArgs.name
nodes = oArgs.nodes
if oArgs.ppn != 8:
    sys.stderr.write('Warning: Forcing ppn to 8. Use --tpn option to limit '
                     'the tasks per node.\n')
ncpus = 8

# Compute the default queue from the walltime
defaultqueue = defqueue(oArgs.walltime)

if oArgs.queue:
    queuespec = '\n#PBS -q ' + oArgs.queue
elif defaultqueue:
    queuespec = '\n#PBS -q ' + defaultqueue
else:
    queuespec = ''

if oArgs.mem:
    memspec = ',mem=%sgb' % oArgs.mem
else:
    memspec = ',mem=%sgb' % (nodes * defaultmempernode)

if oArgs.walltime:
    walltimespec = ',walltime=' + oArgs.walltime
else:
    walltimespec = ''

if oArgs.useprocs:
    nodespec = 'procs=%d,tpn=%d' % (nodes * ncpus, ncpus)
else:
    nodespec = 'nodes=%d:ppn=%d' % (nodes, ncpus)

if ncpus == 8:
    naccesspolicy = ',naccesspolicy=singlejob'
elif oArgs.accesspolicy:
    naccesspolicy = ',naccesspolicy='+oArgs.accesspolicy
else:
    naccesspolicy = ''

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
PBSScript = open(myDir + 'SQDedPBSScriptTemplate.sh').read()%locals()

print PBSScript,
