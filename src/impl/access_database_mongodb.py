from pymongo import MongoClient, errors

from src.data_model import DatabaseObjectException, ErrorMessages
from src.impl.access_database import AccessDatabase


class AccessDatabaseMongoDB(AccessDatabase):

    def __init__(self, connection: str) -> None:
        super(AccessDatabaseMongoDB, self).__init__(connection)
        try:
            client = MongoClient(connection)
            self.db = client['database']
        except errors.ConnectionFailure as e:
            raise(DatabaseObjectException(ErrorMessages.CONNECTION_ERROR + str(e)))

    def get(self, schema: str, object_name: str, id: str = 'all', criteria: str = 'all') -> str:
        """ Devuelve de base de datos el objeto. """
        try:
            collection = self.db[schema]
            output = collection.find_one({"obj_name": object_name})
        except Exception as e:
            raise(DatabaseObjectException(ErrorMessages.GET_ERROR + str(e)))

        return output

    def put(self, schema: str, object_name: str, data: str) -> str:
        """ Envia a base de datos el objeto. """
        try:
            collection = self.db[schema]
            new_data = {'obj_name': object_name, 'data': data}
            result = collection.insert_one(new_data)
            output = result.inserted_id
        except Exception as e:
            raise(DatabaseObjectException(ErrorMessages.PUT_ERROR + str(e)))

        return output

    def update(self, schema: str, object_name: str, data: str) -> str:
        """ Actualiza el objeto con el nuevo data """
        try:
            collection = self.db[schema]
            result = collection.update_one({"obj_name": object_name},  {"$set": data})
            output = result
        except Exception as e:
            raise(DatabaseObjectException(ErrorMessages.PUT_ERROR + str(e)))

        return output

    def remove(self, schema: str, object_name: str) -> str:
        """ Borra el objeto """
        try:
            collection = self.db[schema]
            output = collection.delete_one({"obj_name": object_name})
        except Exception as e:
            raise(DatabaseObjectException(ErrorMessages.REMOVE_ERROR + str(e)))

        return output
