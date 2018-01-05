from database_object_module.data_model import DatabaseObjectException, ErrorMessages
from database_object_module.impl.access_database import AccessDatabase
from database_object_module.impl.access_database_mongodb import AccessDatabaseMongoDB


class AccessDatabaseFactory(object):

    # Static attribute
    _access_database = None

    def __init__(self) -> None:
        pass

    @staticmethod
    def get_access_database(name_database: str, connection_database: str) -> AccessDatabase:
        """
        Get database connection
        :param name_database: name of implemented database
        :param connection_database: url connection
        :return: connection to database
        """

        if AccessDatabaseFactory._access_database is None:
            AccessDatabaseFactory._access_database = AccessDatabaseFactory._implement_database(name_database,
                                                                                               connection_database)
        return AccessDatabaseFactory._access_database

    @staticmethod
    def _implement_database(name_database: str, connection_database: str) -> AccessDatabase:
        """
        Set implemented database
        :param name_database: name of implemented database
        :param connection_database: url connection
        :return: instance of implemented database
        """

        if name_database == 'mongodb':
            return AccessDatabaseMongoDB(connection_database)
        else:
            raise DatabaseObjectException(ErrorMessages.CONFIGURATION_ERROR)
