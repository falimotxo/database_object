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

        # Init the father class
        AccessDatabase.__init__(self, connection)

        try:
            # Connect with mongodb
            self.db = MongoClient(connection).get_database()

        except errors.ConnectionFailure as e:
            raise DatabaseObjectException(ErrorMessages.CONNECTION_ERROR + str(e))

    def get(self, schema: str, conditions: list, criteria: str, native_criteria: bool) -> list:
        """
        Get data from mongodb

        :param schema: schema (name of collection in mongodb)
        :type schema: str

        :param conditions: conditions to search (list of tuple)
        :type conditions: list

        :param criteria: criteria from mongodb
        :type criteria: str

        :param native_criteria: boolean for search by native criteria from mongodb
        :type native_criteria: bool

        :return: list of dictionary with data
        :rtype: list
        """

        # Create the output list
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

            # Return the list with the updated elements
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

        # Create the output list
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

            # Return the list with the updated elements
            return output_list

        except Exception as e:
            raise DatabaseObjectException(ErrorMessages.PUT_ERROR + str(e))

    def update(self, schema: str, data: dict, conditions: list, criteria: str,
               native_criteria: bool) -> list:
        """
        Update data in mongodb

        :param schema: schema (name of collection in mongodb)
        :type schema: str

        :param data: data to store in dictionary format
        :type data: dict

        :param conditions: conditions to search (list of tuple)
        :type conditions: list

        :param criteria: criteria from mongodb
        :type criteria: str

        :param native_criteria: boolean for search by native criteria from mongodb
        :type native_criteria: bool

        :return: list of dictionary with number of updated elements
        :rtype: list
        """

        # Create the output list
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

            # Return the list with the updated elements
            return output_list

        except Exception as e:
            raise DatabaseObjectException(ErrorMessages.PUT_ERROR + str(e))

    def remove(self, schema: str, conditions: list, criteria: str, native_criteria: bool) -> list:
        """
        Delete elements from mongodb

        :param schema: schema (name of collection in mongodb)
        :type schema: str

        :param conditions: conditions to search (list of tuple)
        :type conditions: list

        :param criteria: criteria from mongodb
        :type criteria: str

        :param native_criteria: boolean for search by native criteria from mongodb
        :type native_criteria: bool

        :return: list of dictionary with number of deleted elements
        :rtype: list
        """

        # Create the output list
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

            # Return the list with removed elements.
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

        # If collection exists, get it.
        if schema in self.db.collection_names():
            mongo_collect = self.db[schema]

        # If the function can create it, create it
        elif create_collection:
            mongo_collect = self.db.create_collection(schema)
            mongo_collect.create_index([(AccessDatabase.TIMESTAMP_FIELD, ASCENDING)],
                                       name=AccessDatabase.TIMESTAMP_FIELD, unique=True)

        # In other case, throw the exception
        else:
            raise DatabaseObjectException(ErrorMessages.SCHEMA_ERROR + schema)

        # Return the mongo collection
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

        # Create a dictionary with value of the input data
        mongo_data = dict()
        mongo_data.update(data)

        try:
            # Add the field timestamp if the function should do it
            if create_timestamp:
                timestamp = int(time.time() * 10000000)
                mongo_data[AccessDatabaseMongoDB.TIMESTAMP_FIELD] = timestamp

            # Delete timestamp in other case
            else:
                del mongo_data[AccessDatabaseMongoDB.TIMESTAMP_FIELD]

            # Delete ID key from mongo data, MongoDB will add this field
            del mongo_data[AccessDatabaseMongoDB.ID_FIELD]

            # Return the mongo data
            return mongo_data

        except KeyError as e:
            raise DatabaseObjectException(ErrorMessages.DATA_ERROR + str(e))

    @staticmethod
    def _create_mongo_criteria(conditions: list, criteria: str, native_criteria: bool) -> dict:
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

                # Special case if the variable is the ID
                if condition[0] == AccessDatabase.ID_FIELD:

                    # If the value to compare is a list, one list of ObjectId must be generated
                    if isinstance(condition[2], (list, tuple)):
                        value_compare = [AccessDatabaseMongoDB._str_to_mongoid(element) for element in condition[2]]
                        filter_condition = {
                            condition[0]: {AccessDatabaseMongoDB.MONGO_OPERATORS[condition[1]]: value_compare}
                        }

                    # If the value is empty, all elements must be gotten back
                    elif len(condition[2]) == 0:
                        filter_condition = {}

                    # Else the ObjectId  must be generated
                    else:
                        value_compare = AccessDatabaseMongoDB._str_to_mongoid(condition[2])
                        filter_condition = {
                            condition[0]: {AccessDatabaseMongoDB.MONGO_OPERATORS[condition[1]]: value_compare}
                        }

                # The rest of the variables do not have special cases
                else:
                    value_compare = condition[2]
                    filter_condition = {
                        condition[0]: {AccessDatabaseMongoDB.MONGO_OPERATORS[condition[1]]: value_compare}
                    }

                # Add condition translated to mongodb filter language
                mongo_criteria[AccessDatabaseMongoDB.MONGO_JOIN_CONDITION].append(filter_condition)

            # Only if native criteria is active, add native from user
            if native_criteria and len(criteria) > 0:
                mongo_criteria[AccessDatabaseMongoDB.MONGO_JOIN_CONDITION].append(dict(criteria))

            # Return the mongo criteria
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

            # Return the mongo ID in ObjectId format.
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

        # Return the mongo ID in string format.
        return str(mongo_id)



