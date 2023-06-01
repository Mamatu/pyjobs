import logging
from queue import Queue

from pyjobs.private.pylibcommons.libparameters import verify_parameters
from pyjobs.private.pylibcommons.libprint import print_func_info
from threading import Thread, RLock

import traceback
from enum import Enum

class OutputHandling(Enum):
    NONE = 1,
    AFTER_FUNC = 2,
    AFTER_QUEUE = 3

import traceback

class AsyncQueue(Thread):
    def __init__(self, queue_items, on_exception, on_finish):
        verify_parameters(on_exception, ["ex"])
        verify_parameters(on_finish, [])
        self._stop = False
        self.stop_handler = None
        self.lock = RLock()
        self.queue = []
        for qi in queue_items: self.queue.append(qi)
        self.on_finish_callbacks = [on_finish]
        self.on_exception_callbacks = [on_exception]
        super().__init__(target = self._process_with_exception)
    def _process(self):
        while True:
            func = None
            with self.lock:
                if self._stop:
                    return
                q_item = None
                if len(self.queue) > 0:
                    q_item = self.queue.pop(0)
                if q_item is None:
                    return
                q_out = q_item
                q_stop_handler = None
                q_func = None
                if isinstance(q_out, tuple):
                    q_func, q_stop_handler = q_out
                    print_func_info(extra_string = f"Queue get {q_func} {q_stop_handler}")
                elif callable(q_out):
                    q_func = q_out
                    print_func_info(extra_string = f"Queue get {q_func}")
                else:
                    raise Exception(f"Not supported queue item: {type(q_out)}")
                self.stop_handler = q_stop_handler
                func = q_func
            if func is None:
                raise Exception("func is None")
            print_func_info(extra_string = f"+ Call {func}")
            self.handle_output(func())
            print_func_info(extra_string = f"- Call {func}")
    def put(self, item, index = -1):
        with self.lock:
            if index < 0:
                self.queue.append(item)
            else:
                self.queue.insert(index, item)
    def handle_output(self, output):
        if output is None:
            return
        if isinstance(output, tuple):
            if isinstance(output[0], OutputHandling) and len(output) == 2:
                if output[0] == OutputHandling.NONE:
                    return
                outputs = output[1]
                if not isinstance(outputs, list):
                    outputs = [outputs]
                for o in outputs:
                    if not callable(o):
                        logging.error("Outputs contain not callable item: {o}")
                        return
                with self.lock:
                    if output[0] == OutputHandling.AFTER_FUNC:
                        index = 0
                        for o in outputs:
                            self.put(o, index = index)
                            index = index + 1
                    elif output[0] == OutputHandling.AFTER_QUEUE:
                        for o in outputs: self.put(o)
        else:
            logging.warn(f"Cannot process output: {output}")
    
    def _process_with_exception(self):
        try:
            self._process()
        except Exception as ex:
            logging.fatal(f"Exception {ex} in thread {self}")
            print(f"Exception {ex} in thread {self}")
            traceback.print_tb(ex.__traceback__)
            self.on_exception(ex)
        finally:
            self.on_finish()
    def stop(self):
        with self.lock:
            self._stop = True
            if self.stop_handler:
                self.stop_handler(stop = True)
    def on_finish(self):
        for c in self.on_finish_callbacks: c()
    def on_exception(self, ex):
        for c in self.on_exception_callbacks: c(ex = ex)

def make(queue, on_exception):
    return AsyncQueue(queue, on_exception)
