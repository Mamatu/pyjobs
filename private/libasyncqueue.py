from threading import Thread, Lock
import queue

class AsyncQueue(Thread):
    def __init__(self, queue, on_exception):
        self._stop = False
        self.ids = ids
        self.stop_handler = None
        self.lock = Lock()
        self.q = queue.Queue()
        self.q.queue = queue.deque(queue)
        self.on_exception_callbacks = [on_exception]
        Thread.__init__(self, target = self._process_with_exception, daemon = True)

    def _process(self, *args, **kwargs):
        while True:
            func = None
            with self.lock:
                if self._stop:
                    break
                q_item = self.q.get()
                if q_item is None:
                    break
                q_out = q_item
                q_stop_handler = None
                q_func = None
                if isinstance(q_out, tuple):
                    q_func = q_out[0]
                    q_stop_handler = q_out[1]
                elif isinstance(q_out, callable):
                    q_func = q_out
                else:
                    raise Exception("Not supported queue item")
                self.stop_handler = q_stop_handler
                func = q_func
            if func is None:
                raise Exception("func is None")
            func()
            self.q.task_done()

    def _process_with_exception(self, *args, **kwargs):
        try:
            self._process(self, *args, **kwargs)
        except Exception as ex:
            self.on_exception(ex)

    def stop(self):
        self.lock.acquire()
        try:
            self._stop = True
            if self.stop_handler:
                self.stop_handler(stop = True)
        finally:
            self.lock.release()

    def on_exception(self, ex):
        for c in self.on_exception_callbacks:
            c(ex)

def make(queue, on_exception):
    return AsyncQueue(queue, on_exception)
