#!/usr/bin/env python
# Set the above path as part of the install?

import logging, nws.client, os, signal, socket, subprocess, sys, time
LogC, LogD, LogE, LogI, LogW = logging.critical, logging.debug, logging.error, logging.info, logging.warning

myHost = socket.gethostname()

prog, verbose, logFilePath, key, nwssHost, nwssPort = sys.argv

engineNum = os.getenv('SLURM_PROCID')

outbase = '%s/%s_%s'%(logFilePath, myHost, engineNum)

sys.stderr = open('%s.err'%outbase, 'w', 0)

logging.basicConfig(filename='%s.log'%outbase, format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)

# block some signals to allow sq to shutdown on its own.
def noteSignal(sig, frame):
    LogI('Received signal %d.'%sig)

sigs = [ 1, 2, 3, 15 ]
for x in sigs:
    try: signal.signal(x, noteSignal)
    except: pass

LogI('Wrapper script running on %s (pid: %d, %s).'%(myHost, os.getpid(), engineNum))

ws = nws.client.NetWorkSpace(key, serverHost=nwssHost, serverPort=int(nwssPort))

LogI('Opened workspace %s on %s at %s.'%(key, nwssHost, nwssPort))

# register with the driver.
ws.store('engine info', (myHost, engineNum))

# determine if this engine is needed (pbsdsh starts one eninge for
# each core, so if the user restricts the max tasks per node, some
# engines are not needed).
if ws.fetch('engine %s status'%engineNum) != 'OK':
    LogI('Wrapper script exiting (pid: %d, %s): I\'m not wanted.'%(os.getpid(), engineNum))
    sys.exit(0)

# separate connection to be used by the run task thread.
rtws = nws.client.NetWorkSpace(key, serverHost=nwssHost, serverPort=int(nwssPort))

from threading import Thread

class RunTask(Thread):
    def __init__(self, i, cmd):
        Thread.__init__(self)
        self.i, self.cmd, self.pid = i, cmd, None

    def run(self):
        errf = open('%s_uc.err'%outbase, 'a')
        errf.seek(0, 2)
        outf = open('%s_uc.out'%outbase, 'a')
        outf.seek(0, 2)
        open('%s_split'%outbase, 'a').write('%d %d %d: %s\n'%(self.i, errf.tell(), outf.tell(), repr(self.cmd)))

        startTime = time.time()

        p = subprocess.Popen(['/bin/bash', '-c', self.cmd], stdout=outf, stderr=errf)
        self.pid = p.pid

        # TODO: when should these files be closed?

        LogI('Task %d: child %d started at %f'%(self.i, p.pid,  startTime))

        p.wait()
        self.pid = None

        LogI('Task %d: pid %d returned %d.'%(self.i, p.pid, p.returncode))

        # we need a new connection to avoid stomping on the existing one.
        rtws.store('Task %d Status'%self.i, (p.returncode, startTime, time.time(), 0, p.pid, myHost, engineNum))


taskCount = 0
engineTag = 'engine %s task'%engineNum
rtt = None
while 1:
    try:
        i, task = ws.fetch(engineTag)
    except:
        break

    LogI('Fetched task %d: %s'%(i, task))

    if -1 == i and 'bye' == task: break

    taskCount += 1

    rtt = RunTask(i, task)
    rtt.start()

if rtt and rtt.is_alive():
    try:
        if rtt.pid:
            try:
                LogI('Task %d: pid %d is being tickled.'%(rtt.i, rtt.pid))
                os.kill(rtt.pid, 5)
            except:
                pass
        rtt.join(3) # wait a bit for the thread.
        if rtt.is_alive() and rtt.pid:
            try: 
                os.kill(rtt.pid, 9)
            except:
                pass
            LogI('Task %d: pid %d is being whacked and dummy status generated.'%(rtt.i, rtt.pid))
            ws.store('Task %d Status'%rtt.i, (-1, -1.0, -1.0, 1, rtt.pid, myHost, engineNum))

    except:
        pass # assume that this means the thread exited on its own.

LogI('Wrapper script exiting on %s (pid: %d, %s), "%s" after %d tasks.'%(myHost, os.getpid(), engineNum, engineTag, taskCount))

sys.exit(0)
