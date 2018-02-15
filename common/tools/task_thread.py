import threading

from database_object_module.data_model import ErrorMessages


class TaskThread(threading.Thread):
    """Thread that executes a task every N seconds"""

    def __init__(self):
        threading.Thread.__init__(self)
        self._finished = threading.Event()
        self._interval = 10.0
        self._lock = threading.Lock()

    def setInterval(self, interval):
        """Set the number of seconds we sleep between executing our task"""
        self._interval = interval

    def shutdown(self):
        """Stop this thread"""
        self._finished.set()

    def run(self):
        while True:
            if self._finished.isSet():
                return

            # Execute task
            self._lock.acquire()
            self.task()
            self._lock.release()

            # sleep for interval or until shutdown
            self._finished.wait(self._interval)

    def task(self):
        """The task done by this thread - override in subclasses"""
        raise NotImplementedError(ErrorMessages.REPR_ERROR)
