from threading import Thread, Lock
from relmd_types import PipelineHandling

import signal
import os

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

class PipelineHandling(Enum):
    CLEAR_RESTART = 1,
    APPEND_CONTINUE = 2

import queue

class PipelineElement:
    def __init__(self):
        pass

class ProcessElement(PipelineElement):
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

class _PipelineThread(Thread):
    def __init__(self, ids, pipeline, on_exception):
        self._stop = False
        self.ids = ids
        self.signal_handler = None
        self.lock = Lock()
        self.q = queue.Queue()
        self.q.queue = queue.deque(pipeline)
        self.on_exception_callbacks = [on_exception]
        self.processed_label = ""
        self.done_labels = []
        Thread.__init__(self, target = self._worker, daemon = True)
    def _worker(self, *args, **kwargs):
        try:
            while True:
                self.lock.acquire()
                try:
                    if self._stop:
                        break
                    output = self.q.get()
                    if output is None:
                        break
                    (signal_handler, item) = output
                    self.signal_handler = signal_handler
                finally:
                    self.lock.release()
                self.processed_label = str(item)
                item()
                self.done_labels.append(self.processed_label)
                self.processed_label = ""
                self.q.task_done()
        except Exception as ex:
            self.on_exception(ex)
    def stop(self):
        self.lock.acquire()
        try:
            self._stop = True
            if self.signal_handler:
                self.signal_handler(stop = True)
        finally:
            self.lock.release()
    def on_exception(self, ex):
        for c in self.on_exception_callbacks:
            c(self.ids, ex)
