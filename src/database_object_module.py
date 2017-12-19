from src.data_model import DatabaseObject, DatabaseObjectResult
from src.impl.access_database_factory import AccessDatabaseFactory


class DatabaseObjectModule(object):

    def __init__(self) -> None:
        super().__init__()
        self.access = AccessDatabaseFactory('test', 'testConnection').get_access_database()

    def get(self, schema: str, object_name: str, id='all', criteria='all') -> DatabaseObjectResult:
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

    def __init__(self) -> None:
        super().__init__()
