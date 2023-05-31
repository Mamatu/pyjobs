import logging
from queue import Queue

from pyjobs.private.pylibcommons.libparameters import verify_parameters
from pyjobs.private.pylibcommons.libprint import print_func_info
from threading import Thread, Lock

import traceback

class AsyncQueue(Thread):
    def __init__(self, queue_items, on_exception, on_finish):
        verify_parameters(on_exception, ["ex"])
        verify_parameters(on_finish, [])
        self._stop = False
        self.stop_handler = None
        self.lock = Lock()
        self.queue = Queue()
        for qi in queue_items: self.queue.put(qi)
        self.on_finish_callbacks = [on_finish]
        self.on_exception_callbacks = [on_exception]
        super().__init__(target = self._process_with_exception)
    def _process(self):
        while True:
            func = None
            with self.lock:
                if self._stop:
                    return
                q_item = self.queue.get(False)
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
            func()
            print_func_info(extra_string = f"- Call {func}")
            self.queue.task_done()
    def _process_with_exception(self):
        try:
            self._process()
        except Exception as ex:
            logging.fatal(f"Exception {ex} in thread {self}")
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
