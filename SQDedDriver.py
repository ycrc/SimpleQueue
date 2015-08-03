#!/usr/bin/env python
import logging, nws, os, pnwsst, Queue, signal, subprocess, sys, thread, threading, time
LogC, LogD, LogE, LogI, LogW = logging.critical, logging.debug, logging.error, logging.info, logging.warning

# we assume that the helper script lives in the same directory as this script.
WRAPPER = os.path.dirname(os.path.realpath(__file__))+os.path.sep+'SQDedWrapper.py'

FetchDelay = float(os.environ.get('SQFetchDelay', '.2'))
LaunchDelay = float(os.environ.get('SQLaunchDelay', '.2'))

# XXX Is this purposely trying to generate an error if SQLaunchDelay
# XXX is set to 0?  Seems odd, so I'm commenting it out. -sbw
#if not LaunchDelay: LauncDelay

# this variant is for PBS (or similar) use only.

def launchcmd():
    ecount = 0
    nodefile = os.getenv('PBS_NODEFILE') or os.getenv('LSB_DJOB_HOSTFILE')
    for n in open(nodefile):
        n = n.strip()
        if n and n[0] != '#': ecount += 1

    cmdv = ['mpirun']
    cmdv += ['-n', str(ecount)]
    wdir = os.getenv('HOME')
    if wdir:
        cmdv += ['-wdir', wdir]

    ## It doesn't seem necessary to explicitly propagate LD_LIBRARY_PATH
    ## when using mpirun.

    # ldlibpath = os.getenv('LD_LIBRARY_PATH')
    # if ldlibpath:
    #     cmdv += ['-x', 'LD_LIBRARY_PATH=' + ldlibpath]

    return cmdv

class TaskStream:
    def __init__(self, f):
        self.inflight = {}
        self.monSem = threading.Lock()       # used to implement monitor-like access to an instance.
        self.taskFileName = f
        self.taskgen = None
        self.tc = 0
        self.tf = open(f)
        
    def nexttask(self):
        for l in self.tf:
            l = l.strip()
            if l and (l[0] != '#' or l.startswith('#SQ_OP')):
                c = self.tc
                self.tc = c + 1
                yield (c, l)
        self.tf.close()

    def __iter__(self):
        if not self.taskgen:
            self.taskgen = self.nexttask()
        return self.taskgen

    def setInflight(self, i, info):
        self.monSem.acquire()
        self.inflight[i] = info
        self.monSem.release()

    def setDone(self, i):
        self.monSem.acquire()
        self.inflight.pop(i)
        self.monSem.release()

    def dumpInflight(self):
        LogI('Inflight:')
        self.monSem.acquire()
        for x in sorted(self.inflight.keys()):
            LogI('\t%d: %s'%(x, repr(self.inflight[x])))
        self.monSem.release()
        
    def dumpRemaining(self):
        # TODO: make sure this is done under a lock!
        # doing this even when all tasks are "done" avoids stale REMAINING files.
        dumpfile=self.taskFileName+'.REMAINING'
        LogI('Remaining tasks dumped to %s.'%dumpfile)

        f = open(dumpfile, 'w')
        for x in sorted(self.inflight.keys()):
            f.write('%s\n'% self.inflight.pop(x)[-1])

        for x, t in self.taskgen:
            f.write('%s\n'%t)
        f.close()
        
class EnginePool:
    def __init__(self, ws):
        self.activeEngines = {}
        
        self.draining = False

        self.engines = {}
        self.en2h = {}
        self.engineQueue = Queue.Queue(0)

        self.monSem = threading.Lock()       # used to implement monitor-like access to an instance.

        self.drainCV = threading.Condition(self.monSem)

        self.receivedShutdown = False

        # we only learn from this how many engines to expect to hear from.
        ecount = 0
        nodefile = os.getenv('PBS_NODEFILE') or os.getenv('LSB_DJOB_HOSTFILE')
        for n in open(nodefile):
            n = n.strip()
            if n and n[0] != '#': ecount += 1

        limit = oArgs.maxTasksPerNode
        h2e = {}
        wsSem.acquire()
        for x in xrange(ecount):
            eh, en = ws.fetch('engine info')
            ee = h2e.get(eh, [])
            if len(ee) < limit:
                ee.append(en)
                ws.store('engine %s status'%en, 'OK')
                h2e[eh] = ee
                self.en2h[en] = eh
                self.engineQueue.put(en)
                self.engines[en] = True
            else:
                ws.store('engine %s status'%en, 'GO AWAY')
        wsSem.release()

        LogI('Nodelist: %s' % sorted(h2e.keys()))

    def drainPool(self):
        # wait for all assigned tasks to finish. this uses mon sem
        self.drainCV.acquire()
        self.draining = True
        while self.activeEngines:
            self.drainCV.wait()
        self.draining = False
        self.drainCV.release()

    def getEngine(self, taskIndex):
        # return an engine that can be assigned a new task --- block if
        # necessary for one to become available.
        en = self.engineQueue.get(True)
        if en == -1:
            self.engineQueue.put(en)
        else:
            self.monSem.acquire()
            if self.draining:
                LogI('Got an engine while draining!!!')
            self.activeEngines[en] = taskIndex
            self.monSem.release()

        return en

    def releaseNode(self, engineNum, tStream, i, completed=True):
        self.monSem.acquire()
        LogI('Releasing %s (%s) for task %d (%s).'%(engineNum, self.en2h[engineNum], i, completed))

        self.activeEngines.pop(engineNum)
        if completed: tStream.setDone(i)
        if self.draining:
            if not self.activeEngines:
                LogI('Notify drainCV.')
                self.drainCV.notify()

        if not self.receivedShutdown:
            self.engineQueue.put(engineNum)

        self.monSem.release()

    def shutdown(self):
        self.monSem.acquire()

        # this should only be called by the signal handler.
        LogI('In shutdown.')

        doPoisonTasks = False

        # only do this once.
        if not self.receivedShutdown:
            doPoisonTasks = True
            self.receivedShutdown = True
            # empty engine queue.
            while not self.engineQueue.empty(): self.engineQueue.get()
            LogI('Engine queue is now empty.')
            self.engineQueue.put(-1)

        self.monSem.release()
        LogI('Poison engine in engine queue.')

        # since this can trigger engine releases, we prefer not to do it while holding the monitor.
        if doPoisonTasks:
            wsSem.acquire()
            for en in self.engines:
                ws.store('engine %s task'%en, (-1, 'bye'))
            wsSem.release()
        LogI('Poison tasks sent.')
            
        
def runTask(ep, en, tStream, i, task, key):
    task = task.strip()
    if task[0] in '\'"':
        LogW('removing quotes from: '+task)
        task = task[1:-1]

    LogI('Launching on %s (%s): %s'%(en, ep.en2h[en], task))
    tStream.setInflight(i, (en, ep.en2h[en], task))
    wsSem.acquire()
    ws.store('engine %s task'%en, (i, task))
    ws.store('Launched', str(int(ws.fetch('Launched'))+1))
    wsSem.release()
        
    # be careful in the following not to block or blow up.
    wsSem.acquire()
    try:
        me = 'Task %d Status'%i
        LogI('Fetching extra data for task %d from workspace.'%i)
        while 1:
            status, startTime, stopTime, rogue, pid, host, engineNum = ws.fetchTry(me, (-1, -1., -1., True, -1, 'no where', -1))
            if pid != -1: break
            wsSem.release()
            time.sleep(FetchDelay)
            wsSem.acquire()
        try:    ws.deleteVar(me)
        except: pass
        if status != None: status = divmod(status, 256)
        LogI('Extra data for task %d from workspace: %s %f %f %s %d %s'%(i, status, startTime, stopTime, rogue, pid, host))
        taskStatus.write('%d\t%s\t%f\t%f\t%d\t%d\t%s\t%s\n'%(i, status, startTime, stopTime, rogue, pid, host, task))
        if rogue:
            # "borrowing" the wsSem to protect this write.
            taskRogues.write('%s\t%d\n'%(host, pid))
        ws.store('Done', str(int(ws.fetch('Done')) + 1))
        if status == 0:
            ws.store('Succeeded', str(int(ws.fetch('Succeeded')) + 1))
        else:
            ws.store('Failed', str(int(ws.fetch('Failed')) + 1))
    except Exception, e:
        LogI('Encountered exception "%s" while accessing extra status info for task %d.'%(e, i))
    wsSem.release()
        
    if status == (0, 0):
        # Everything OK.
        ep.releaseNode(engineNum, tStream, i)
    elif status and status[0] == 0 and oArgs.ignoreErrors:
        # Means the task exited with a non-zero exit code, but we've been told to ignore that.
        ep.releaseNode(engineNum, tStream, i)
    else:
        # Either the task was terminated, it exited with a non-zero
        # code which we are not ignoring, or it may be a rogue. Flag
        # as incomplete.
        ep.releaseNode(engineNum, tStream, i, completed=False)

def runTasks(ep, tStream, key):
    allLaunched = False
    # TODO: this could now block, e.g. if the task file is a named
    # pipe. overall flow needs to be modified to handle blocking here
    # correctly.
    for i, task in tStream:
        if task.startswith('#SQ_OP'):
            op = task.split()
            if len(op) == 2 and op[1].upper() == 'DRAIN':
                LogI('Initiating drain (%d).'%i)
                ep.drainPool()
                LogI('Done drain (%d).'%i)
                #tStream.setDone(i) # don't do this --- it's not a real task.
            else:
                LogW('Ignoring unrecognized SimpleQueue operation: %s'%task)
            continue
        # must call getEngine in this context *before* thread is
        # created. this serializes calls to getEngine, as well as
        # calls to drainPool.
        en = ep.getEngine(i)
        if en == -1: break
        thread.start_new_thread(runTask, (ep, en, tStream, i, task, key))
        time.sleep(LaunchDelay)
    else:
        LogI('All tasks launched.')
        allLaunched = True

    if not allLaunched: LogI('Looks like we\'re shutting down, so skipping tasks from index %d on.'%i)
    LogI('Draining pool.')
    ep.drainPool()

def setupLogging(logLevel, logFile):
    logging.basicConfig(level=logLevel,
                        filename=logFile,
                        filemode='w',
                        format='%(asctime)s.%(msecs)03d %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
                                           
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter=logging.Formatter('%(levelname)s %(asctime)s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


if '__main__' == __name__:
    import optparse

    nwssSet = False
    def monitor_store(option, opt_str, value, parser):
        global nwssSet
        nwssSet = True
        setattr(parser.values, option.dest, value)

    opts = optparse.OptionParser(description='Process a simple queue of tasks using a dedicated collection of nodes.',
                                 usage='usage: %prog [options] TaskFile')
    opts.add_option('-H', '--nwssHost', help='nws server host (%default).', action='callback', callback=monitor_store, metavar='Host', default='localhost') 
    opts.add_option('-i', '--ignoreErrors', help='Consider a task done even if it returns an error code.', action='store_true', default=False)
    opts.add_option('-l', '--logFile', help='Specify log file name (%default).', metavar='LogFile', default='log.out') 
    opts.add_option('--maxTasksPerNode', help='When running with PBS, limit tasks to N per node.',  metavar='N', type='int', default=1000000) 
    opts.add_option('-n', '--nodeFiles', help='Comma separated list of files listing nodes to use (%default).', metavar='FileList', default='$PBS_NODEFILE')
    opts.add_option('-p', '--nwssPort', help='nws server port (%default).', action='callback', callback=monitor_store, metavar='Port', type='int', default='8765') 
    opts.add_option('--pnwss', help='Create a personal workspace for this run.', action='store_true', default=False)
    opts.add_option('-v', '--verbose', action='store_const', const=logging.DEBUG, default=logging.INFO)
    opts.add_option('-V', '--wrapperVerbose', action='store_true', default=False)
       
    oArgs, pArgs = opts.parse_args()
    if len(pArgs) != 1:
        # Eventually change to allow multiple task files, with some sort of indication of which file a task comes from.
        print >>sys.stderr, 'Need one (and only one) task file.'
        opts.print_help(sys.stderr)
        sys.exit(1)

    setupLogging(oArgs.verbose, oArgs.logFile)
    if oArgs.pnwss:
        if nwssSet: LogW('pnwss overrides nwssHost and nwssPort.')
        pnwss = pnwsst.NwsLocalServer(logFile=oArgs.logFile+'.pnwss')
        LogI('Started personal networkspace server: %s %d %d'%(pnwss.host, pnwss.port, pnwss.webport))
        setattr(oArgs, 'nwssHost', pnwss.host)
        setattr(oArgs, 'nwssPort', pnwss.port)

    LogI('Control process is %d.'%os.getpid())
    LogI('sqDedicated run started using nws server %s %d'%(oArgs.nwssHost, oArgs.nwssPort))
    
    key=('SENTINEL %s %s %s' % (os.environ['LOGNAME'], time.asctime(), pArgs[0])).replace(' ', '_')
    LogI('Sentinel key: %s' % key)

    ws = nws.client.NetWorkSpace(key, serverHost=oArgs.nwssHost, serverPort=oArgs.nwssPort)
    ws.store('Launched', '0')
    ws.store('Done', '0')
    ws.store('Failed', '0')
    ws.store('Succeeded', '0')

    # needed to synchronize access by threads running tasks.
    wsSem = threading.Lock()

    logFilePath = os.path.dirname(os.path.realpath(oArgs.logFile)) # all other log files are placed in the same directory as the main log file.

    cmdv = launchcmd() + \
            [sys.executable, WRAPPER, str(oArgs.wrapperVerbose),
            logFilePath, key, oArgs.nwssHost, str(oArgs.nwssPort)]
    LogI('Launching workers with command: ' + ' '.join(cmdv))
    launcherp = subprocess.Popen(cmdv,
            stdout=open('%s/launcher.out'%logFilePath, 'w'),
            stderr=open('%s/launcher.err'%logFilePath, 'w'))

    LogI('launcher pid: %d' % launcherp.pid)

    # launcher command will launch the engines, collect info from them.
    ep = EnginePool(ws)


    tStream = TaskStream(pArgs[0])

    taskStatus = open(tStream.taskFileName+'.STATUS', 'w', 0)
    taskRogues = open(tStream.taskFileName+'.ROGUES', 'w', 0)

    goodbye = False

    def runThread():
        runTasks(ep, tStream, key)
        LogI('runTasks has finished.')
        taskRogues.close()
        taskStatus.close()
        
        tStream.dumpRemaining()
        LogI('Run completed.')
        pnwss.stop()
        global goodbye
        goodbye = True
        os.kill(os.getpid(), signal.SIGTERM)

    rt = threading.Thread(None, runThread)
    rt.start()

    def noteSignal(sig, frame):
        LogI('Ignoring signal %d.'%sig)
        
    for x in range(1, signal.NSIG):
        if x == signal.SIGCHLD: continue
        try: signal.signal(x, noteSignal)
        except: pass

    def shutItDown(sig, frame):
        # add signal.alarm to as a deadman switch?
        if goodbye:
            LogI('Received signal %d, but already shutting down.'%sig)
            return
        else:
            LogI('Received signal %d. Shutting down.'%sig)

        tStream.dumpInflight()

        ep.shutdown()
        
    sigs = [ 1, 2, 3, 15 ]
    for x in sigs:
        try: signal.signal(x, shutItDown)
        except: pass

    while 1:
        signal.pause()
        if goodbye: break

    LogI('Falling off the end of the world.')
