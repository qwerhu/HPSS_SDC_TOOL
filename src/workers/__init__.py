# coding=utf-8
import os
import uuid
import time
import psutil
import logging
import ctx
from threading import Thread
from multiprocessing import Process


PROCESS_STATE_PEDING = 'PENDING'
PROCESS_STATE_RUNNING = 'RUNNING'
PROCESS_STATE_ABORT = 'ABORT'
PROCESS_STATE_HALT = 'HALT'
WORKER_MODE_PROCESS = 'process'
WORKER_MODE_THRED = 'thread'


def process_loop(m, *args, **kwargs):
    p = psutil.Process(os.getpid())
    if not isinstance(p.ppid, int):
        ppid = p.ppid()
    else:
        ppid = p.ppid
    if ppid > 0:
        p = psutil.Process(ppid)

        def check():
            if not p.is_running():
                logging.error('process dead, exiting...')
                os._exit(-1)

        import schedule
        schedule.logger.setLevel('WARN')
        schedule.every(1).seconds.do(check)

        def __check():
            while True:
                schedule.run_pending()
                time.sleep(0.5)

        Thread(target=__check).start()
    return m(*args, **kwargs)


class Worker(object):

    def is_alive(self):
        return self.module and self.module.is_alive()


class ProcessWorker(Worker):
    def __init__(self, target, name=None, args=(), kwArgs={}, protect=True, sys=True, cid=None):
        self.id = uuid.uuid4().get_hex()
        if cid is not None:
            self.id = cid
        self.name = name
        self.target = target
        self.args = args
        self.kwArgs = kwArgs
        self.protect = protect
        self.type = 'Process'
        self.state = PROCESS_STATE_PEDING
        self.reboot = -1
        self.sys = sys
        self.module = None

    @property
    def pid(self):
        return self.module.pid

    def start(self):
        import importlib
        if isinstance(self.target, basestring):
            m = importlib.import_module(self.target)
        else:
            m = self.target
        self.module = Process(target=process_loop, name=self.name, args=(m,) + self.args, kwargs=self.kwArgs)
        self.module.daemon = True
        try:
            ctx.mongodb.close()
            self.module.start()
            self.state = PROCESS_STATE_RUNNING
            self.reboot += 1
        except Exception as err:
            self.state = PROCESS_STATE_ABORT
            logging.exception(err)

    def terminate(self):
        self.state = PROCESS_STATE_PEDING
        if self.module is not None:
            self.module.terminate()


class ThreadWorker(Worker):
    def __init__(self, target, name=None, args=(), kwArgs={}, protect=True, sys=True, cid=None):
        self.id = uuid.uuid4().get_hex()
        if cid is not None:
            self.id = cid
        self.name = name
        self.target = target
        self.type = 'Thread'
        self.args = args
        self.kwArgs = kwArgs
        self.protect = protect
        self.state = PROCESS_STATE_PEDING
        self.reboot = -1
        self.sys = sys
        self.module = None
        self.pid = os.getpid()
        return

    def start(self):
        import importlib
        if isinstance(self.target, basestring):
            m = importlib.import_module(self.target)
        else:
            m = self.target
        self.module = Thread(target=m, name=self.name, args=self.args, kwargs=self.kwArgs)
        try:
            self.module.start()
            self.state = PROCESS_STATE_RUNNING
            self.reboot += 1
        except Exception as e:
            self.state = PROCESS_STATE_ABORT
            logging.exception(e)

    def terminate(self):
        logging.warn('Cannot terminate thread.')

