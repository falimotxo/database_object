import abc


class AccessDatabase(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, connection: str) -> None:
        self.connection = connection

    @abc.abstractmethod
    def get(self, schema: str, object_name: str, id='all', criteria='all') -> str:
        pass

    @abc.abstractmethod
    def put(self, schema: str, object_name: str, data: str) -> str:
        pass

    @abc.abstractmethod
    def update(self, schema: str, object_name: str, data: str) -> str:
        pass

    @abc.abstractmethod
    def remove(self, schema: str, object_name: str) -> str:
        pass

    def get_connection(self) -> str:
        return self.connection
