from configparser import ConfigParser
from os import path

from src.data_model import DatabaseObject, DatabaseObjectResult, DatabaseObjectException, ErrorMessages
from src.impl.access_database_factory import AccessDatabaseFactory


class DatabaseObjectModule(object):

    def __init__(self) -> None:
        super().__init__()
        config = DatabaseConfigureModule()
        name_database = config.get_value('name_database')
        connection_database = config.get_value('connection_database')
        self.access = AccessDatabaseFactory(name_database, connection_database).get_access_database()

    def get(self, schema: str, object_name: str, id: str = 'all', criteria: str = 'all') -> DatabaseObjectResult:
        ret = self.access.get(schema, object_name, id, criteria)
        return self._get_data_object_result_from_json('get', ret)

    def put_object(self, schema: str, object_name: str, data: DatabaseObject) -> DatabaseObjectResult:
        return self.put(schema, object_name, data.__dict__)

    def put(self, schema: str, object_name: str, data) -> DatabaseObjectResult:
        ret = self.access.put(schema, object_name, data)
        return self._get_data_object_result_from_json('put', ret)

    def update(self, schema: str, object_name: str, data) -> DatabaseObjectResult:
        ret = self.access.update(schema, object_name, data.__dict__)
        return self._get_data_object_result_from_json('update', ret)

    def remove(self, schema: str, object_name: str) -> DatabaseObjectResult:
        ret = self.access.remove(schema, object_name)
        return self._get_data_object_result_from_json('remove', ret)

    def _get_data_object_result_from_json(self, from_method: str, json: str) -> DatabaseObjectResult:
        pass


class DatabaseConfigureModule(object):
    PATH_CONFIGURE_FILE = '/etc/config.ini'

    def __init__(self) -> None:
        super().__init__()
        self.config = ConfigParser()
        self.config.read(path.dirname(__file__) + DatabaseConfigureModule.PATH_CONFIGURE_FILE)

    def get_sections(self) -> list:
        return self.config.sections()

    def get_value(self, key: str, section: str = 'DEFAULT') -> str:
        try:
            return self.config[section][key]
        except Exception as e:
            raise (DatabaseObjectException(ErrorMessages.KEYFILE_ERROR + str(e)))
