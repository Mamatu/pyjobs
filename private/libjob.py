import logging
from enum import Enum

class JobHandling(Enum):
    CLEAR_RESTART = 1,
    APPEND_CONTINUE = 2

class JobStatus(Enum):
    STARTED = 1,
    FINISHED = 2

from pyjobs.private.pylibcommons.libparameters import verify_parameters
from pyjobs.private.pylibcommons.libprint import class_debug_prints

class JobsProcess:
    def __init__(self):
        self._jobs = {}
        self._jobs_status = {}
        self._update_id = {}
        self._exceptions = {}
        self._callbacks = []
        self.update_id_exception = False
    def register_callback(callback):
        verify_parameters(callback, ["job_id", "job_status"])
        self._callbacks.append(callback)
    def unregister_callback(callback):
        if callback not in self._callbacks:
            raise Exception(f"Callback {callback} was not registered")
        self._callbacks.remove(callback)
    def on_thread_exception(self, job_id, ex):
        self._exceptions[job_id] = ex
    def on_thread_finish(self, job_id):
        logging.info(f"Job {job_id} finished")
    def _run_job_thread(self, job_id, update_id, pipeline_items):
        if job_id in self._jobs:
            raise Exception(f"Is already job thread with job_id {job_id}")
        jobThread = _JobThread(job_id, update_id, pipeline_items, on_exception = self.on_thread_exception, on_finish = self.on_thread_finish)
        self._jobs[job_id] = jobThread
        jobThread.start()
        job_status = JobStatus.STARTED
        self._jobs_status[job_id] = job_status
        self._call_callbacks(job_id = job_id, job_status = job_status)
    def _call_callbacks(self, job_id, job_status):
        for c in self._callbacks: c(job_id = job_id, job_status = job_status)
    def _handle_append_continue(self, job_id, update_id, pipeline_items):
        if job_id in self._jobs:
            for pipeline_item in pipeline_items:
                self._jobs[job_id].q.put(pipeline_item)
        else:
            self._run_job_thread(job_id, update_id, pipeline_items)
    def _handle_clear_restart(self, job_id, update_id, pipeline_items):
        if job_id in self._jobs:
            job = _jobs[job_id]
            job.stop()
        self._run_job_thread(job_id, update_id, pipeline_items)
    def enable_update_id_exception(self, update_id_exception = True):
        self.update_id_exception = update_id_exception
    def process_pipelines(self, pipelines):
        if isinstance(pipelines, dict):
            for k,v in pipelines.items():
                if isinstance(k, tuple) and len(k) == 2:
                    self.process_pipeline((k[0], k[1], v))
                else:
                    raise Exception("Not supported key type")
        else:
            raise Exception("Not supported pipelines type")
    def process_pipeline(self, pipeline):
        handlers = {}
        handlers[JobHandling.APPEND_CONTINUE] = self._handle_append_continue
        handlers[JobHandling.CLEAR_RESTART] = self._handle_clear_restart
        exception_msg = f"pipeline_getter must return tuple of type (job_id, update_id, jobs) or (job_id, update_id, jobs, handling_type)"
        if not isinstance(pipeline, tuple):
            raise Exception(exception_msg)
        if not len(pipeline) == 3 or len(pipeline) == 4:
            raise Exception(exception_msg)
        handling = JobHandling.APPEND_CONTINUE
        if len(pipeline) == 4:
            handling = pipeline[3]
        job_id, update_id, pipeline_items = pipeline
        if self._update_id.get(job_id, None) == update_id:
            msg = f"pipeline {pipeline} has already processed update_id {update_id}. Nothing to do"
            if self.update_id_exception:
                raise Exception(msg)
            logging.warning(msg)
            return
        self._update_id[job_id] = update_id
        if not handling in handlers:
            raise Exception(f"Not supported handling {handling}")
        handlers[handling](job_id, update_id, pipeline_items)
    def wait_for_finish(self, job_id):
        self._jobs[job_id].wait_for_finish()

from pyjobs.private.libasyncqueue import AsyncQueue
from threading import Thread
from multiprocessing import Condition

class _JobThread(AsyncQueue):
    def __init__(self, job_id, update_id, queue, on_exception, on_finish):
        self._job_id = job_id
        self._update_id = update_id
        self._on_exception = on_exception
        self._on_finish = on_finish
        self._is_finished = False
        self._cond = Condition()
        super().__init__(queue, on_exception = self.on_exception, on_finish = self.on_finish)
    def on_exception(self, ex):
        self._on_exception(self._job_id, ex)
    def on_finish(self):
        self._on_finish(self._job_id)
        with self._cond:
            self._is_finished = True
            self._cond.notify_all()
    def wait_for_finish(self):
        with self._cond:
            if not self._is_finished:
                self._cond.wait()

import signal
from dataclasses import dataclass
from typing import Any

class _SignalHandler:
    @dataclass
    class Process:
        proc: Any
        signal_to_stop: Any
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

def _kill(p):
    parent = psutil.Process(p.pid)
    children = parent.children(recursive=True)
    for child in children:
        child.kill()
    logging.info(f"kill {p}")
    p.kill()
    p.wait()

def parse_pipeline_file(path):
    with open(path, "r") as file:
        code = compile(file.read(), f"{path}", 'exec')
        ex_locals = {}
        exec(code, None, ex_locals)
        if not 'pipelines' in ex_locals:
            raise Exception(f"No pipelines in {path}")
        return ex_locals['pipelines']
