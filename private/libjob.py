from threading import Thread, Lock
from relmd_types import JobHandling

import signal
import os

from pyjobs.private import libasyncqueue

_threads = {}
_pipelines = {}
_exceptions = {}

class _SignalHandler:
    class Process:
        def __init__(self, proc, signal_to_stop):
            self.proc = proc
            self.signal_to_stop = signal_to_stop
        def stop(self):
            self.proc.send_signal(self.signal_to_stop)
    def __init__(self):
        self.items = []
        self._stopped = False
    def register_proc(self, proc, signal_to_stop = signal.SIGINT):
        self.items.append(SignalHandler.Process(proc, signal_to_stop))
    def __call__(self, **kwargs):
        if "stop" in kwargs and kwargs['stop'] == True:
            self._stopped = True
            for i in self.items:
                i.stop()
            self.items.clear()
    def stopped(self):
        return self._stopped

from enum import Enum

class JobHandling(Enum):
    CLEAR_RESTART = 1,
    APPEND_CONTINUE = 2

class JobElement:
    def __init__(self):
        pass

class ProcessElement(JobElement):
    def __init__(self):
        pass

def _kill(p):
    parent = psutil.Process(p.pid)
    children = parent.children(recursive=True)
    for child in children:
        child.kill()
    logging.info(f"kill {p}")
    p.kill()
    p.wait()

class _Job:
    def __init__(self, update_id, job_id, pipeline = []):
        self.update_id = update_id
        self.job_id = job_id
        self.pipeline = pipeline
        self.job_queue = libasyncqueue.make(self.pipeline)

class _Jobs:
    def __init__(self, jobs_file_path):
        pass

def handle_pipelines_file(pipelines_path):
    global _threads, _pipelines, _exceptions
    with open(pipelines_path, "r") as rpipelines_file:
        code = compile(pipelines_file.read(), f"{pipeline_path}", 'exec')
        ex_locals = {}
        exec(code, None, ex_locals)
        pipelines = ex_locals['pipelines']
        for ids, callables in pipelines.items():
            def on_thread_exception(ids, ex):
                _exceptions[ids[1]] = ex
            (iid, pid) = ids
            if _pipelines.get(pid, None) == iid:
                continue
            _pipelines[pid] = iid
            handling = JobHandling.APPEND_CONTINUE
            if isinstance(callables, tuple):
                handling = callables[0]
                callables = callables[1]
            if handling is JobHandling.APPEND_CONTINUE:
                if pid in _threads:
                    for c in callables:
                        _threads[pid].q.put(c)
                else:
                    thread = _JobThread(ids, callables, on_exception = on_thread_exception)
                    _threads[pid] = thread
                    thread.start()
            elif handling is JobHandling.CLEAR_RESTART:
                if pid in _threads:
                    thread = _threads[pid]
                    thread.stop()
                thread = _JobThread(ids, callables, on_exception = on_thread_exception)
                _threads[pid] = thread
                thread.start()
