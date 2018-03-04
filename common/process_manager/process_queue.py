import uuid
from common.tools.task_thread import TaskThread
from common.process_manager.process_manager_model import AlgorithmType, StatusType, Process, ProcessManagerException, \
    ErrorMessages


class ProcessQueue:

    def __init__(self, algorithm: AlgorithmType = AlgorithmType.SEQUENTIAL) -> None:
        """
        Constructor
        :param algorithm: algorithm that sorts process execution
        """

        # Store algorithm, process list, process outputs and cache
        self.algorithm = algorithm
        self.process_list = list()
        self.process_id_set = set()
        self.output = dict()
        self.cache = dict()

        # Process queue lock. It is used for allow new process
        self.locked = False

        # Task for monitoring process list
        self.monitor = MonitorTask(self)
        self.monitor.setInterval(10)
        self.monitor.start()

    def add_process(self, process: Process) -> None:
        """
        Add process to the list
        :param process: process to add
        :return: None or exception
        """

        if self.locked:
            raise ProcessManagerException(ErrorMessages.LOCKED_QUEUE)

        # Generate id, set to process, set status and add to the list
        process_id = int(uuid.uuid1())
        process.set_id(process_id)
        process.set_status(StatusType.IDLE)
        self.process_list.append(process)
        self.process_id_set.add(process_id)

    def get_ouput(self, process_id: int) -> object:
        """
        Get output of a process
        :param process_id: id to return
        :return: data
        """

        if process_id not in self.process_id_set:
            raise ProcessManagerException(ErrorMessages.PROCESS_NOT_EXIST)
        elif process_id not in self.output:
            raise ProcessManagerException(ErrorMessages.NOT_OUTPUT_YET)

        output = self.output[process_id]
        return output

    def get_cache(self) -> dict:
        """
        Get all cache
        :return: cache
        """

        return self.cache

    def set_cache(self, values: dict) -> None:
        """
        Set values to cache
        :param values: values to cache
        :return: None
        """

        self.cache = values

    def del_cache(self, cache_id: str = None) -> None:
        """
        Delete cache positions or all cache
        :param cache_id: id to delete
        :return: None
        """

        if cache_id:
            self.cache = dict()
        else:
            del(self.cache[cache_id])

    def lock(self):
        self.locked = True

    def unlock(self):
        self.locked = False

    def delete(self, now: bool = False):
        """
        Ordered shutdown of the queue
        :param now: stop pending process or not
        :return: None
        """

        self.lock()
        self.monitor.shutdown()

    def is_ready_for_delete(self) -> bool:
        """
        Monitor running tasks
        :return: True if all process there are not running processes
        """

        for process in self.process_list:
            if process.status == StatusType.RUNNING:
                return False

class MonitorTask(TaskThread):

    def __init__(self, process_queue: ProcessQueue) -> None:
        TaskThread.__init__(self)
        self.process_queue = process_queue

    def task(self) -> None:
        pass
