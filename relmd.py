from threading import Thread, Lock
from relmd_types import PipelineHandling

import time
import queue
_threads = {}
_pipelines = {}
_exceptions = {}

def _kill(p):
    parent = psutil.Process(p.pid)
    children = parent.children(recursive=True)
    for child in children:
        child.kill()
    logging.info(f"kill {p}")
    p.kill()
    p.wait()

import importlib

class PipelineThread(Thread):
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

_previous_str_status = ""

def print_status():
    import traceback
    global _previous_str_status
    _str_status = ""
    import os
    for ids, thread in _threads.items():
        _str_status += f"--------------------------------------------------\n"
        _str_status += f"{thread.done_labels} -> {thread.processed_label}\n"
        if ids in _exceptions:
            exception = _exceptions[ids]
            _str_status += f"{exception}\n"
            stackSummary = traceback.extract_tb(exception.__traceback__)
            stackSummary = stackSummary.format()
            stackSummary = "".join(stackSummary)
            _str_status += f"{stackSummary}\n"
    if _str_status != _previous_str_status:
        _previous_str_status = _str_status
        os.system("clear")
        print(_str_status)

def manage_pipelines():
    global _threads, _pipelines, _exceptions
    with open("relmd_pipelines.py", "r") as rpipelines_file:
        code = compile(rpipelines_file.read(), 'relmd_pipelines.py', 'exec')
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
            handling = PipelineHandling.APPEND_CONTINUE
            if isinstance(callables, tuple):
                handling = callables[0]
                callables = callables[1]
            if handling is PipelineHandling.APPEND_CONTINUE:
                if pid in _threads:
                    for c in callables:
                        _threads[pid].q.put(c)
                else:
                    thread = PipelineThread(ids, callables, on_exception = on_thread_exception)
                    _threads[pid] = thread
                    thread.start()
            elif handling is PipelineHandling.CLEAR_RESTART:
                if pid in _threads:
                    thread = _threads[pid]
                    thread.stop()
                thread = PipelineThread(ids, callables, on_exception = on_thread_exception)
                _threads[pid] = thread
                thread.start()

_stop_manager = False

def run():
    global _stop_manager
    import traceback, os
    _last_exception = (None, None)
    while not _stop_manager:
        manage_pipelines()
        print_status()
        time.sleep(2)

def _signal_handler(sig, frame):
    global _stop_manager
    _stop_manager = True
    import psutil, os
    parent = psutil.Process(os.getpid())
    children = parent.children(recursive=True)
    for child in children:
        child.kill()

if __name__ == "__main__":
    import signal
    signal.signal(signal.SIGINT, _signal_handler)
    run()
