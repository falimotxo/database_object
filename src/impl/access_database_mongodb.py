from bson.objectid import ObjectId, InvalidId
from pymongo import MongoClient, collection, errors, TEXT

from src.data_model import DatabaseObjectException, ErrorMessages
from src.impl.access_database import AccessDatabase


class AccessDatabaseMongoDB(AccessDatabase):
    """
    Class to define the specific access to the MongoDB database.
    """

    NAME_FIELD = '_obj_name'
    ID_FIELD = '_id'
    COMPARISON_DICT = {'=': '$eq', '>': '$gt', '>=': '$gte', 'in': '$in',
                       '<': '$lt', '<=': '$lte', '!=': '$ne', 'out': '$nin'}

    def __init__(self, connection: str) -> None:
        super(AccessDatabaseMongoDB, self).__init__(connection)
        try:
            self.db = MongoClient(connection).get_database()
        except errors.ConnectionFailure as e:
            raise DatabaseObjectException(ErrorMessages.CONNECTION_ERROR + str(e))

    def get(self, schema: str, object_name: str, condition: tuple, criteria: str, native_criteria: bool) -> list:
        output_list = list()
        try:
            mongo_collect = self._get_collect(schema)
            mongo_criteria = AccessDatabaseMongoDB._create_mongo_criteria(object_name, condition, criteria,
                                                                          native_criteria)
            mongo_result = mongo_collect.find(mongo_criteria)
            for element in mongo_result:
                str_id = AccessDatabaseMongoDB._mongoid_to_str(element[AccessDatabaseMongoDB.ID_FIELD])
                element[AccessDatabaseMongoDB.ID_FIELD] = str_id
                del element[AccessDatabaseMongoDB.NAME_FIELD]
                output_list.append(element)
        except Exception as e:
            raise DatabaseObjectException(ErrorMessages.GET_ERROR + str(e))

        return output_list

    def put(self, schema: str, object_name: str, data: dict) -> list:
        output_list = list()
        try:
            mongo_collect = self._get_collect(schema, create=True)
            mongo_data = AccessDatabaseMongoDB._create_mongo_data(object_name, data)
            mongo_id = mongo_collect.insert_one(mongo_data).inserted_id
            str_id = AccessDatabaseMongoDB._mongoid_to_str(mongo_id)
            output_list.append({AccessDatabaseMongoDB.ID_FIELD: str_id})
        except Exception as e:
            raise DatabaseObjectException(ErrorMessages.PUT_ERROR + str(e))

        return output_list

    def update(self, schema: str, object_name: str, data: dict, condition: tuple, criteria: str,
               native_criteria: bool) -> list:
        output_list = list()
        try:
            mongo_collect = self._get_collect(schema)
            mongo_data = AccessDatabaseMongoDB._create_mongo_data(object_name, data)
            mongo_criteria = AccessDatabaseMongoDB._create_mongo_criteria(object_name, condition, criteria,
                                                                          native_criteria)
            mongo_result = mongo_collect.update_many(mongo_criteria, {'$set': mongo_data}).modified_count
            output_list.append({'modified_count': mongo_result})
        except Exception as e:
            raise DatabaseObjectException(ErrorMessages.PUT_ERROR + str(e))

        return output_list

    def remove(self, schema: str, object_name: str, condition: tuple, criteria: str, native_criteria: bool) -> list:
        output_list = list()
        try:
            mongo_collect = self._get_collect(schema)
            mongo_criteria = AccessDatabaseMongoDB._create_mongo_criteria(object_name, condition, criteria,
                                                                          native_criteria)
            mongo_result = mongo_collect.delete_many(mongo_criteria).deleted_count
            output_list.append({'deleted_count': mongo_result})
        except Exception as e:
            raise DatabaseObjectException(ErrorMessages.REMOVE_ERROR + str(e))

        return output_list

    def _get_collect(self, schema: str, create: bool = False) -> collection:
        if schema in self.db.collection_names():
            mongo_collect = self.db[schema]
        elif create:
            mongo_collect = self.db.create_collection(schema)
            mongo_collect.create_index([(AccessDatabaseMongoDB.NAME_FIELD, TEXT)],
                                       name=AccessDatabaseMongoDB.NAME_FIELD)
        else:
            raise DatabaseObjectException(ErrorMessages.SCHEMA_ERROR + schema)

        return mongo_collect

    @staticmethod
    def _create_mongo_data(object_name: str, data: dict) -> dict:
        mongo_data = {AccessDatabaseMongoDB.NAME_FIELD: object_name}
        mongo_data.update(data)
        if len(mongo_data[AccessDatabaseMongoDB.ID_FIELD]) > 0:
            mongo_data[AccessDatabaseMongoDB.ID_FIELD] = AccessDatabaseMongoDB._str_to_mongoid(
                mongo_data[AccessDatabaseMongoDB.ID_FIELD])
        else:
            del mongo_data[AccessDatabaseMongoDB.ID_FIELD]

        return mongo_data

    @staticmethod
    def _create_mongo_criteria(object_name: str, condition: tuple, criteria: str, native_criteria: bool) -> dict:
        mongo_criteria = dict()
        try:
            if condition[0] == AccessDatabaseMongoDB.ID_FIELD:
                if isinstance(condition[2], list):
                    value_compare = [AccessDatabaseMongoDB._str_to_mongoid(element) for element in condition[2]]
                else:
                    value_compare = AccessDatabaseMongoDB._str_to_mongoid(condition[2])
            else:
                value_compare = condition[2]
            mongo_criteria.update({
                '$and': [
                    {AccessDatabaseMongoDB.NAME_FIELD: {AccessDatabaseMongoDB.COMPARISON_DICT['=']: object_name}},
                    {condition[0]: {AccessDatabaseMongoDB.COMPARISON_DICT[condition[1]]: value_compare}}
                ]
            })
            if native_criteria and len(criteria) > 0:
                mongo_criteria['$and'].append(dict(criteria))
        except Exception as e:
            raise DatabaseObjectException(ErrorMessages.CRITERIA_ERROR + str(e))

        return mongo_criteria

    @staticmethod
    def _str_to_mongoid(str_id: str) -> ObjectId:
        mongo_id = None
        try:
            if len(str_id) > 12:
                mongo_id = ObjectId(bytes.fromhex(str_id))
            elif len(str_id) > 0:
                mongo_id = ObjectId(str_id.zfill(12).encode().hex())
        except (InvalidId, ValueError) as e:
            raise DatabaseObjectException(ErrorMessages.ID_ERROR + str(e))

        return mongo_id

    @staticmethod
    def _mongoid_to_str(mongo_id: ObjectId) -> str:
        try:
            str_id = bytes.fromhex(str(mongo_id)).decode().lstrip('0')
            if not str_id.isprintable():
                str_id = str(mongo_id)
            elif len(str_id) <= 0:
                str_id = '0'
        except UnicodeDecodeError:
            str_id = str(mongo_id)

        return str_id
