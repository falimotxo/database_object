from src.data_model import DatabaseObjectException, ErrorMessages
from src.impl.access_database import AccessDatabase
from src.impl.access_database_mongodb import AccessDatabaseMongoDB


class AccessDatabaseFactory(object):
    _access_database = None

    def __init__(self) -> None:
        super(AccessDatabaseFactory, self).__init__()

    @staticmethod
    def get_access_database(name_database: str, connection_database: str) -> AccessDatabase:
        if AccessDatabaseFactory._access_database is None:
            AccessDatabaseFactory._access_database = AccessDatabaseFactory._implement_database(name_database,
                                                                                               connection_database)
        return AccessDatabaseFactory._access_database

    @staticmethod
    def _implement_database(name_database: str, connection_database: str) -> AccessDatabase:
        if name_database == 'mongodb':
            return AccessDatabaseMongoDB(connection_database)
        else:
            raise DatabaseObjectException(ErrorMessages.CONFIGURATION_ERROR)
