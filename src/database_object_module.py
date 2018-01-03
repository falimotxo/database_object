from configparser import ConfigParser
from os import path

from src.data_model import DatabaseObject, DatabaseObjectResult, DatabaseObjectException, ErrorMessages
from src.impl.access_database_factory import AccessDatabaseFactory


class DatabaseObjectModule(object):

    def __init__(self) -> None:
        super(DatabaseObjectModule, self).__init__()
        config = DatabaseConfigureModule()
        name_database = config.get_value('name_database')
        connection_database = config.get_value('connection_database')
        self.access_db = AccessDatabaseFactory.get_access_database(name_database, connection_database)

    def get(self, schema: str, object_name: str, condition: tuple = ('_id', '!=', ''),
            criteria: str = '', native_criteria: bool = False) -> DatabaseObjectResult:
        ret = self.access_db.get(schema, object_name, condition, criteria, native_criteria)
        return DatabaseObjectModule._get_data_object_result_from_json('get', ret)

    def put_object(self, schema: str, object_name: str, data: DatabaseObject) -> DatabaseObjectResult:
        return self.put(schema, object_name, data.__dict__)

    def put(self, schema: str, object_name: str, data: dict) -> DatabaseObjectResult:
        ret = self.access_db.put(schema, object_name, data)
        return DatabaseObjectModule._get_data_object_result_from_json('put', ret)

    def update_object(self, schema: str, object_name: str, data: DatabaseObject, condition: tuple = ('_id', '!=', ''),
                      criteria: str = '', native_criteria: bool = False) -> DatabaseObjectResult:
        return self.update(schema, object_name, data.__dict__, condition, criteria, native_criteria)

    def update(self, schema: str, object_name: str, data: dict, condition: tuple = ('_id,' '!=', ''),
               criteria: str = '', native_criteria: bool = False) -> DatabaseObjectResult:
        ret = self.access_db.update(schema, object_name, data, condition, criteria, native_criteria)
        return DatabaseObjectModule._get_data_object_result_from_json('update', ret)

    def remove(self, schema: str, object_name: str, condition: tuple = ('_id', '!=', ''),
               criteria: str = '', native_criteria: bool = False) -> DatabaseObjectResult:
        ret = self.access_db.remove(schema, object_name, condition, criteria, native_criteria)
        return DatabaseObjectModule._get_data_object_result_from_json('remove', ret)

    @staticmethod
    def _get_data_object_result_from_json(from_method: str, result: list) -> DatabaseObjectResult:
        return DatabaseObjectResult(from_method, str(result))


class DatabaseConfigureModule(object):
    PATH_CONFIGURE_FILE = '/../etc/config.ini'

    def __init__(self) -> None:
        super(DatabaseConfigureModule, self).__init__()
        self.config = ConfigParser()
        self.config.read(path.dirname(__file__) + DatabaseConfigureModule.PATH_CONFIGURE_FILE)

    def get_sections(self) -> list:
        return self.config.sections()

    def get_value(self, key: str, section: str = 'DEFAULT') -> str:
        try:
            return self.config[section][key]
        except Exception as e:
            raise DatabaseObjectException(ErrorMessages.KEYFILE_ERROR + str(e))
