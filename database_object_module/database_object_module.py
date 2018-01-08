import configparser
import os

from database_object_module.data_model import DatabaseObjectResult, DatabaseObjectException, ErrorMessages, \
    DatabaseObject
from database_object_module.impl.access_database_factory import AccessDatabaseFactory


class DatabaseObjectModule(object):
    """
    Main class of database access
    """

    def __init__(self) -> None:
        """
        Constructor that gets configuration, database name and connection instance
        """

        config = DatabaseConfigureModule()
        name_database = config.get_value('name_database')
        connection_database = config.get_value('connection_database')
        self.access_db = AccessDatabaseFactory.get_access_database(name_database, connection_database)

    def get(self, schema: str, object_name: str, conditions: list = (('_id', '!=', ''),), criteria: str = '',
            native_criteria: bool = False) -> DatabaseObjectResult:
        """
        Get data from data store

        :param schema: connection schema
        :type schema: str

        :param object_name: object type to save
        :type object_name: str

        :param conditions: list of tuple conditions
        :type conditions: list

        :param criteria: criteria search
        :type criteria: str

        :param native_criteria: criteria native from database
        :type native_criteria: bool

        :return: database object result
        :rtype: DatabaseObjectResult
        """

        try:
            schema_collection = schema + '_' + object_name
            ret = self.access_db.get(schema_collection, conditions, criteria, native_criteria)
            return DatabaseObjectModule._get_data_object_result_from_json('get', object_name, result=ret)

        except DatabaseObjectException as e:
            return DatabaseObjectModule._get_data_object_result_from_json('get', object_name, exception=e)

    def put_object(self, schema: str, object_name: str, data: DatabaseObject) -> DatabaseObjectResult:
        """
        Write object to object store

        :param schema: connection schema
        :type schema: str

        :param object_name: object type to save
        :type object_name: str

        :param data: data to save
        :type data: DatabaseObject

        :return: database object result with _id
        :rtype: DatabaseObjectResult
        """

        # Validate data, checking that has inheritance from DatabaseObject
        if not issubclass(data.__class__, DatabaseObject):
            e = DatabaseObjectException(ErrorMessages.INHERITANCE_ERROR)
            return DatabaseObjectModule._get_data_object_result_from_json('put', object_name, exception=e)

        return self.put(schema, object_name, data.__dict__)

    def put(self, schema: str, object_name: str, data: dict) -> DatabaseObjectResult:
        """
        Write object to object store

        :param schema: connection schema
        :type schema: str

        :param object_name: object type to save
        :type object_name: str

        :param data: data to save
        :type data: dict

        :return: database object result with _id
        :rtype: DatabaseObjectResult
        """

        try:
            # Validate data, checking that object has mandatory fields
            self._validate_data(data)
            schema_collection = schema + '_' + object_name
            ret = self.access_db.put(schema_collection, data)
            return DatabaseObjectModule._get_data_object_result_from_json('put', object_name, result=ret)

        except DatabaseObjectException as e:
            return DatabaseObjectModule._get_data_object_result_from_json('put', object_name, exception=e)

    def update_object(self, schema: str, object_name: str, data: DatabaseObject,
                      conditions: list = (('_id', '!=', ''),), criteria: str = '',
                      native_criteria: bool = False) -> DatabaseObjectResult:
        """
        Update data in object store. This method will update all object data defined in "data" attribute

        :param schema: connection schema
        :type schema: str

        :param object_name: object type to save
        :type object_name: str

        :param data: data to update
        :type data: DatabaseObject

        :param conditions: list of tuple conditions
        :type conditions: list

        :param criteria: criteria to search
        :type criteria: str

        :param native_criteria: use native criteria from database or not
        :type native_criteria: bool

        :return: data object result wit number of updated objects
        :rtype: DatabaseObjectResult
        """

        # Validate data, checking that has inheritance from DatabaseObject
        if not issubclass(data.__class__, DatabaseObject):
            e = DatabaseObjectException(ErrorMessages.INHERITANCE_ERROR)
            return DatabaseObjectModule._get_data_object_result_from_json('update', object_name, exception=e)

        # This updates all object data defined in "data" attribute because we pass __dict__ to update method
        return self.update(schema, object_name, data.__dict__, conditions, criteria, native_criteria)

    def update(self, schema: str, object_name: str, data: dict, conditions: list = (('_id,' '!=', ''),),
               criteria: str = '', native_criteria: bool = False) -> DatabaseObjectResult:
        """
        Update data in object store. . This method will update only fields defined in "data" attribute

        :param schema: connection schema
        :type schema: str

        :param object_name: object type to save
        :type object_name: str

        :param data: data to update
        :type data: dict

        :param conditions: list of tuple conditions
        :type conditions: list

        :param criteria: criteria to search
        :type criteria: str

        :param native_criteria: use native criteria from database or not
        :type native_criteria: bool

        :return: data object result wit number of updated objects
        :rtype: DatabaseObjectResult
        """

        try:
            schema_collection = schema + '_' + object_name
            ret = self.access_db.update(schema_collection, data, conditions, criteria, native_criteria)
            return DatabaseObjectModule._get_data_object_result_from_json('update', object_name, result=ret)

        except DatabaseObjectException as e:
            return DatabaseObjectModule._get_data_object_result_from_json('update', object_name, exception=e)

    def remove(self, schema: str, object_name: str, conditions: list = (('_id', '!=', ''),), criteria: str = '',
               native_criteria: bool = False) -> DatabaseObjectResult:
        """
        Remove data in object store

        :param schema: connection schema
        :type schema: str

        :param object_name: object type to save
        :type object_name: str

        :param conditions: list of tuple conditions
        :type conditions: list

        :param criteria: criteria to search
        :type criteria: str

        :param native_criteria: use native criteria from database or not
        :type native_criteria: bool

        :return: data object result wit number of deleted objects
        :rtype: DatabaseObjectResult
        """

        try:
            schema_collection = schema + '_' + object_name
            ret = self.access_db.remove(schema_collection, conditions, criteria, native_criteria)
            return DatabaseObjectModule._get_data_object_result_from_json('remove', object_name, result=ret)

        except DatabaseObjectException as e:
            return DatabaseObjectModule._get_data_object_result_from_json('remove', object_name, exception=e)

    @staticmethod
    def _validate_data(data: dict) -> None:
        """
        Check of data inherit from DatabaseObject

        :param data: data to check
        :type data: dict

        :return: This function return nothing
        :rtype: None
        """

        if not all(key in data.keys() for key in vars(DatabaseObject())):
            raise DatabaseObjectException(ErrorMessages.INHERITANCE_ERROR)

    @staticmethod
    def _get_data_object_result_from_json(from_method: str, object_name: str, result: list = None,
                                          exception: Exception = None) -> DatabaseObjectResult:
        """
        Convert data from implemented database to data object result
        :param from_method: method which has been executed
        :param result: data returned by method
        :return: database object result with information about output
        """

        # TODO

        # Detected exception. It is convenient to detect all type of exceptions
        if exception is not None:
            return DatabaseObjectResult(DatabaseObjectResult.CODE_KO, object_name, msg=str(exception),
                                        exception=exception)

        if list is not None:
            if from_method in ['get', 'remove', 'update', 'put']:
                return DatabaseObjectResult(DatabaseObjectResult.CODE_OK, object_name, data=str(result))
            else:
                return DatabaseObjectResult(DatabaseObjectResult.CODE_KO, object_name, data='')


class DatabaseConfigureModule(object):
    """
    Class to get the configuration options
    """

    # Path configuration of file
    PATH_CONFIGURE_FILE = '/../etc/config.ini'

    def __init__(self) -> None:
        """
        Constructor: read properties file
        """

        self.config = configparser.ConfigParser()
        self.config.read(os.path.dirname(__file__) + DatabaseConfigureModule.PATH_CONFIGURE_FILE)

    def get_sections(self) -> list:
        """
        Recover sections of config

        :return: sections of config
        :rtype: list
        """

        return self.config.sections()

    def get_value(self, key: str, section: str = 'DEFAULT') -> str:
        """
        Recover value from section

        :param key: key of the value
        :type key: str

        :param section: section
        :type section: str

        :return: value
        :rtype: str
        """

        try:
            return self.config[section][key]
        except Exception as e:
            raise DatabaseObjectException(ErrorMessages.KEYFILE_ERROR + str(e))
