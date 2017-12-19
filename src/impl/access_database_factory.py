from src.data_model import DatabaseObjectException, ErrorMessages
from src.impl.access_database import AccessDatabase
from src.impl.access_database_mongodb import AccessDatabaseMongoDB


class AccessDatabaseFactory(object):

    def __init__(self, name_database: str, connection_database: str) -> None:
        super().__init__()
        self.access_database = None
        self.name_database = name_database
        self.connection_database = connection_database

    def get_access_database(self) -> AccessDatabase:
        if self.access_database is not None:
            return self.access_database

        self.access_database = self._implement_database()
        return self.access_database

    def _implement_database(self) -> AccessDatabase:
        if self.name_database == 'mongodb':
            return AccessDatabaseMongoDB(self.connection_database)

        else:
            raise (DatabaseObjectException(ErrorMessages.CONFIGURATION_ERROR))
