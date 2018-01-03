import abc


class AccessDatabase(object):
    """
    Abstract class to define the function header of all the access implementations to the different databases.
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, connection: str) -> None:
        """
        Builder method of the class.

        :param connection: name of the connection with the database
        :type connection: str

        :return: this function return nothing
        :rtype: None
        """
        super(AccessDatabase, self).__init__()
        self.connection = connection

    @abc.abstractmethod
    def get(self, schema: str, object_name: str, condition: tuple, criteria: str, native_criteria: bool) -> list:
        """
        Get the object from the database.

        :param schema: name of schema of the database
        :type schema: str

        :param object_name: name of the object to get
        :type object_name: str

        :param condition: native condition to get the object
        :type condition: tuple

        :param criteria: advanced condition for complex searches, can be native or generic
        :type criteria: str

        :param native_criteria: select between native criteria or generic criteria
        :type native_criteria: bool

        :return: list of objects gotten as a dictionary
        :rtype: list
        """
        pass

    @abc.abstractmethod
    def put(self, schema: str, object_name: str, data: dict) -> list:
        """
        Put the object into the database.

        :param schema: name of schema of the database
        :type schema: str

        :param object_name: name of the object to put
        :type object_name: str

        :param data: list of objects to put into the database
        :type data: list

        :return: ----------------
        :rtype: list
        """
        pass

    @abc.abstractmethod
    def update(self, schema: str, object_name: str, data: dict, condition: tuple, criteria: str,
               native_criteria: bool) -> list:
        """
        Update the object from the database.

        :param schema: name of schema of the database
        :type schema: str

        :param object_name: name of the object to update
        :type object_name: str

        :param data: list of objects to update into the database
        :type data: list

        :param condition: native condition to update the object
        :type condition: tuple

        :param criteria: advanced condition for complex searches, can be native or generic
        :type criteria: str

        :param native_criteria: select between native criteria or generic criteria
        :type native_criteria: bool

        :return: ----------------
        :rtype: list
        """
        pass

    @abc.abstractmethod
    def remove(self, schema: str, object_name: str, condition: tuple, criteria: str, native_criteria: bool) -> list:
        """
        Update the object from the database.

        :param schema: name of schema of the database
        :type schema: str

        :param object_name: name of the object to remove
        :type object_name: str

        :param condition: native condition to update the object
        :type condition: tuple

        :param criteria: advanced condition for complex searches, can be native or generic
        :type criteria: str

        :param native_criteria: select between native criteria or generic criteria
        :type native_criteria: bool

        :return: ----------------
        :rtype: list
        """
        pass

    def get_connection(self) -> str:
        """
        Get the name of the database connection.

        :return: the name of the database connection
        :rtype: str
        """
        return self.connection
