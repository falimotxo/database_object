import abc


class AccessDatabase(object):
    """
    Abstract class to define the function header of all the access implementations to the different databases.
    """

    __metaclass__ = abc.ABCMeta

    # Field _id
    ID_FIELD = '_id'

    # # Field _timestamp
    TIMESTAMP_FIELD = '_timestamp'

    def __init__(self, connection: str) -> None:
        """
        Builder method of the class.

        :param connection: name of the connection with the database
        :type connection: str

        :return: this function return nothing
        :rtype: None
        """

        self.connection = connection

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
        Update the object from the database.

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
