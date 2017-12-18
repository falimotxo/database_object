from access_database_mongodb import AccessDatabaseMongoDB
from access_database import AccessDatabase


class AccessDatabaseFactory(object):
    def __init__(self) -> None:
        super().__init__()
        self.connected = False
        self.access_database = None

    def get_access_database(self) -> AccessDatabase:
        if self.connected:
            return self.access_database

        connection = 'mongodb://localhost:27017'
        self.access_database = AccessDatabaseMongoDB(connection)
        self.connected = True
        return self.access_database