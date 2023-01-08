from enum import Enum

import signal
import os

class SignalHandler:
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

class PipelineHandling(Enum):
    CLEAR_RESTART = 1,
    APPEND_CONTINUE = 2
