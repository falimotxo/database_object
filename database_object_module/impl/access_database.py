import abc
import time

from database_object_module.data_model import DatabaseObjectException, ErrorMessages


class AccessDatabase(object):
    """
    Abstract class to define the function header of all the access implementations to the different databases.
    """

    __metaclass__ = abc.ABCMeta

    # Field _id
    ID_FIELD = '_id'

    # Field _timestamp
    TIMESTAMP_FIELD = '_timestamp'

    # Field _deleted_count
    DELETED_COUNT = '_deleted_count'

    # Field _deleted_count
    UPDATED_COUNT = '_updated_count'

    def __init__(self, connection_url: str) -> None:
        """
        Builder method of the class.

        :param connection_url: name of the connection with the database
        :type connection_url: str

        :return: this function return nothing
        :rtype: None
        """

        self.connection = None
        self.connection_url = connection_url

    @abc.abstractmethod
    def get(self, schema: str, conditions: list, criteria: str, native_criteria: bool) -> list:
        """
        Get the object from the database.

        :param schema: name of schema of the database
        :type schema: str

        :param conditions: conditions to update the object (list of tuples)
        :type conditions: list

        :param criteria: advanced condition for complex searches, can be native or generic
        :type criteria: str

        :param native_criteria: select between native criteria or generic criteria
        :type native_criteria: bool

        :return: list of objects gotten as a dictionary
        :rtype: list of dictionary

        """
        pass

    @abc.abstractmethod
    def put(self, schema: str, data: dict) -> list:
        """
        Put the object into the database.

        :param schema: name of schema of the database
        :type schema: str

        :param data: dict of objects to put into the database
        :type data: dict

        :return: list with inserted _id
        :rtype: list of dictionary
        """

        pass

    @abc.abstractmethod
    def update(self, schema: str, data: dict, conditions: list, criteria: str,
               native_criteria: bool) -> list:
        """
        Update the object from the database.

        :param schema: name of schema of the database
        :type schema: str

        :param data: list of objects to update into the database
        :type data: list

        :param conditions: conditions to update the object (list of tuples)
        :type conditions: list

        :param criteria: advanced condition for complex searches, can be native or generic
        :type criteria: str

        :param native_criteria: select between native criteria or generic criteria
        :type native_criteria: bool

        :return: number of updated elements in a list of dictionary
        :rtype: list of dictionary
        """

        pass

    @abc.abstractmethod
    def remove(self, schema: str, conditions: list, criteria: str, native_criteria: bool) -> list:
        """
        Remove the objects from the database.

        :param schema: name of schema of the database
        :type schema: str

        :param conditions: conditions to update the object (list of tuples)
        :type conditions: list

        :param criteria: advanced condition for complex searches, can be native or generic
        :type criteria: str

        :param native_criteria: select between native criteria or generic criteria
        :type native_criteria: bool

        :return: number of removed elements in a list of dictionary
        :rtype: list of dictionary
        """

        pass

    @abc.abstractclassmethod
    def _connect_database(self) -> None:
        pass

    @abc.abstractclassmethod
    def _close_database(self) -> None:
        pass

    @abc.abstractclassmethod
    def _check_connection(self) -> bool:
        """
        Check that the connection is alive.

        :return:
        :rtype: None
        """

        pass

    def recover_connection(self, recovery_attempts: int, wait_seconds: int) -> None:
        """
        Check that the connection is alive and try to recover it not.

        :return: this function return nothing
        :rtype: None
        """

        current_attemps = 0
        while not self._check_connection():
            try:
                self._close_database()
                time.sleep(0.01)
                self._connect_database()

            except DatabaseObjectException:
                current_attemps += 1
                if current_attemps > recovery_attempts:
                    raise DatabaseObjectException(ErrorMessages.CONNECTION_ERROR)

                time.sleep(wait_seconds)
