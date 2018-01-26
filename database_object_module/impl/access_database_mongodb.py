import time

from pymongo import MongoClient, collection, errors, ASCENDING

from database_object_module.data_model import DatabaseObjectException, ErrorMessages
from database_object_module.impl.access_database import AccessDatabase


class AccessDatabaseMongoDB(AccessDatabase):
    """
    Class to define the specific access to the MongoDB database.
    """

    # Operators
    MONGO_OPERATORS = {
        '=': '$eq', '>': '$gt', '>=': '$gte', 'in': '$in', '<': '$lt', '<=': '$lte', '!=': '$ne', 'out': '$nin'
    }

    # Mongo join condition
    MONGO_JOIN_CONDITION = '$and'

    # Mongo update operators
    MONGO_UPDATE_OPERATOR = '$set'

    def __init__(self, connection: str) -> None:
        """
        Constructor with url connection

        :param connection: url connection from ini file
        :type connection: str

        :return: This function return nothing
        :rtype: None
        """

        AccessDatabase.__init__(self, connection)

        # Connect with mongodb
        try:
            self.db = MongoClient(connection).get_database()
        except errors.ConnectionFailure as e:
            raise DatabaseObjectException(ErrorMessages.CONNECTION_ERROR + str(e))

    def get(self, schema: str, conditions: tuple, criteria: str, native_criteria: bool) -> list:
        """
        Get data from mongodb

        :param schema: schema (name of collection in mongodb)
        :type schema: str

        :param conditions: conditions to search (tuple of tuple)
        :type conditions: list

        :param criteria: criteria from mongodb
        :type criteria: str

        :param native_criteria: boolean for search by native criteria from mongodb
        :type native_criteria: bool

        :return: list of dictionary with data
        :rtype: list
        """

        output_list = list()

        try:
            # Get collection
            mongo_collect = self._get_collection(schema)

            # Get criteria in mongodb language
            mongo_criteria = AccessDatabaseMongoDB._create_mongo_criteria(conditions, criteria, native_criteria)

            # Find data with criteria
            mongo_result = mongo_collect.find(mongo_criteria).sort(AccessDatabaseMongoDB.TIMESTAMP_FIELD)

            # For each data: recover _id, set to data and add to output list
            for element in mongo_result:
                str_id = AccessDatabaseMongoDB._mongoid_to_str(element[AccessDatabaseMongoDB.ID_FIELD])
                element[AccessDatabaseMongoDB.ID_FIELD] = str_id
                output_list.append(element)

            return output_list

        except Exception as e:
            raise DatabaseObjectException(ErrorMessages.GET_ERROR + str(e))

    def put(self, schema: str, data: dict) -> list:
        """
        Insert data to mongodb

        :param schema: schema (name of collection in mongodb)
        :type schema: str

        :param data: data to store in dictionary format
        :type data: dict

        :return: list of dictionary with inserted _id
        :rtype: list
        """

        output_list = list()

        try:
            # Get collection creating it if not exists
            mongo_collect = self._get_collection(schema, create_collection=True)

            # Change _id to ObjectId because mongodb needs it
            mongo_data = AccessDatabaseMongoDB._create_mongo_data(data, create_timestamp=True)

            # Insert data and recover _id
            mongo_result = mongo_collect.insert_one(mongo_data)
            mongo_id = mongo_result.inserted_id

            # Convert _id from ObjectId native format for _id to String, and add to output list in dictionary format
            str_id = AccessDatabaseMongoDB._mongoid_to_str(mongo_id)
            output_list.append({AccessDatabaseMongoDB.ID_FIELD: str_id})

            return output_list

        except Exception as e:
            raise DatabaseObjectException(ErrorMessages.PUT_ERROR + str(e))

    def update(self, schema: str, data: dict, conditions: tuple, criteria: str,
               native_criteria: bool) -> list:
        """
        Update data in mongodb

        :param schema: schema (name of collection in mongodb)
        :type schema: str

        :param data: data to store in dictionary format
        :type data: dict

        :param conditions: conditions to search (tuple of tuple)
        :type conditions: tuple

        :param criteria: criteria from mongodb
        :type criteria: str

        :param native_criteria: boolean for search by native criteria from mongodb
        :type native_criteria: bool

        :return: list of dictionary with number of updated elements
        :rtype: list
        """

        output_list = list()

        try:
            # Get collection from mongodb
            mongo_collect = self._get_collection(schema)

            # Change _id to ObjectId because mongodb needs it
            mongo_data = AccessDatabaseMongoDB._create_mongo_data(data)
            mongo_data_update = {AccessDatabaseMongoDB.MONGO_UPDATE_OPERATOR: mongo_data}

            # Get criteria in mongodb language
            mongo_criteria = AccessDatabaseMongoDB._create_mongo_criteria(conditions, criteria, native_criteria)

            # Update elements and recover number of updated elements and number of matched elements
            mongo_result = mongo_collect.update_many(mongo_criteria, mongo_data_update)
            modified_count = mongo_result.modified_count
            matched_count = mongo_result.matched_count

            # Add number of updated elements
            output_list.append({'modified_count': modified_count, 'matched_count': matched_count})

            return output_list

        except Exception as e:
            raise DatabaseObjectException(ErrorMessages.PUT_ERROR + str(e))

    def remove(self, schema: str, conditions: tuple, criteria: str, native_criteria: bool) -> list:
        """
        Delete elements from mongodb

        :param schema: schema (name of collection in mongodb)
        :type schema: str

        :param conditions: conditions to search (tuple of tuple)
        :type conditions: tuple

        :param criteria: criteria from mongodb
        :type criteria: str

        :param native_criteria: boolean for search by native criteria from mongodb
        :type native_criteria: bool

        :return: list of dictionary with number of deleted elements
        :rtype: list
        """

        output_list = list()

        try:
            # Get collection from mongodb
            mongo_collect = self._get_collection(schema)

            # Get criteria in mongodb language
            mongo_criteria = AccessDatabaseMongoDB._create_mongo_criteria(conditions, criteria, native_criteria)

            # Delete elements with criteria and recover number of deleted elements
            mongo_result = mongo_collect.delete_many(mongo_criteria)
            deleted_count = mongo_result.deleted_count

            # Add number of deleted elements
            output_list.append({'deleted_count': deleted_count})

            return output_list

        except Exception as e:
            raise DatabaseObjectException(ErrorMessages.REMOVE_ERROR + str(e))

    def _get_collection(self, schema: str, create_collection: bool = False) -> collection.Collection:
        """
        Get collection from database

        :param schema: collection of mongodb
        :type schema: str

        :param create_collection: create collection or not
        :type create_collection: bool

        :return: collection from mongodb
        :rtype: collection
        """

        # If collection exists, get it. Else create it
        if schema in self.db.collection_names():
            mongo_collect = self.db[schema]
        elif create_collection:
            mongo_collect = self.db.create_collection(schema)
            mongo_collect.create_index([(AccessDatabase.TIMESTAMP_FIELD, ASCENDING)],
                                       name=AccessDatabase.TIMESTAMP_FIELD, unique=True)
        else:
            raise DatabaseObjectException(ErrorMessages.SCHEMA_ERROR + schema)

        return mongo_collect

    @staticmethod
    def _create_mongo_data(data: dict, create_timestamp: bool = False) -> dict:
        """
        Create a data with mongo format

        :param data: data to insert into the database
        :type data: dict

        :param create_timestamp: create timestamp or not
        :type create_timestamp: bool

        :return: data with mongo format
        :rtype: dict
        """

        mongo_data = dict()
        mongo_data.update(data)

        if create_timestamp:
            timestamp = int(time.time() * 10000000)
            mongo_data[AccessDatabaseMongoDB.TIMESTAMP_FIELD] = timestamp
        else:
            del mongo_data[AccessDatabaseMongoDB.TIMESTAMP_FIELD]

        del mongo_data[AccessDatabaseMongoDB.ID_FIELD]

        return mongo_data

    @staticmethod
    def _create_mongo_criteria(conditions: tuple, criteria: str, native_criteria: bool) -> dict:
        """
        Create criteria native of mongodb

        :param conditions: list of tuple of conditions
        :type conditions: list

        :param criteria: native criteria language from mongodb
        :type criteria: str

        :param native_criteria: bool for use native criteria or not
        :type native_criteria: bool

        :return: filter
        :rtype: dict
        """

        # Create a filter with list of empty conditions
        mongo_criteria = {AccessDatabaseMongoDB.MONGO_JOIN_CONDITION: list()}

        try:
            # Iterate all conditions to create native mongodb filter
            for condition in conditions:

                if condition[0] == AccessDatabase.ID_FIELD:
                    if isinstance(condition[2], list):
                        value_compare = [AccessDatabaseMongoDB._str_to_mongoid(element) for element in condition[2]]
                        filter = {condition[0]: {AccessDatabaseMongoDB.MONGO_OPERATORS[condition[1]]: value_compare}}
                    elif condition[2] == '':
                        filter = {}
                    else:
                        value_compare = AccessDatabaseMongoDB._str_to_mongoid(condition[2])
                        filter = {condition[0]: {AccessDatabaseMongoDB.MONGO_OPERATORS[condition[1]]: value_compare}}
                else:
                    value_compare = condition[2]
                    filter = {condition[0]: {AccessDatabaseMongoDB.MONGO_OPERATORS[condition[1]]: value_compare}}

                # Add condition translated to mongodb filter language
                mongo_criteria[AccessDatabaseMongoDB.MONGO_JOIN_CONDITION].append(filter)

            # Only if native criteria is active, add native from user
            if native_criteria and len(criteria) > 0:
                mongo_criteria[AccessDatabaseMongoDB.MONGO_JOIN_CONDITION].append(dict(criteria))

            return mongo_criteria

        except Exception as e:
            raise DatabaseObjectException(ErrorMessages.CRITERIA_ERROR + str(e))

    @staticmethod
    def _str_to_mongoid(str_id: str) -> collection.ObjectId:
        """
        Create mongo_id from str_id

        :param str_id: id of the object in string format
        :type str_id: str

        :return: id of the object in ObjectId format
        :rtype: ObjectId
        """

        try:
            # Generate ObjectId from hex of the input string
            mongo_id = collection.ObjectId(bytes.fromhex(str_id))
            return mongo_id

        except (errors.InvalidId, ValueError) as e:
            raise DatabaseObjectException(ErrorMessages.ID_ERROR + str(e))

    @staticmethod
    def _mongoid_to_str(mongo_id: collection.ObjectId) -> str:
        """
        Generate str_id from mongo_id

        :param mongo_id: id of the object in ObjectId format
        :type mongo_id: ObjectId

        :return: id of the object in string format
        :rtype: str
        """

        return str(mongo_id)
