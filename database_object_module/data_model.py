import ast, types

class DatabaseObject(object):

    def __init__(self) -> None:
        self._id = ''
        self._timestamp = None

    def get_id(self) -> str:
        return self._id

    def get_timestamp(self) -> int:
        return self._timestamp

    def __repr__(self):
        # This method is used for store objects inside objects. repr must return string in dictionary format
        raise NotImplementedError(ErrorMessages.REPR_ERROR)


class DatabaseObjectResult(object):

    CODE_OK = 'OK'
    CODE_KO = 'KO'

    def __init__(self, code: str, object_name: str, data: str = '', msg: str = '', exception: Exception = None) -> None:
        self.code_list = (DatabaseObjectResult.CODE_OK, DatabaseObjectResult.CODE_KO)
        self.code = code
        self.object_name = object_name
        self.data = data
        self.msg = msg
        self.exception = exception

    def get_object_from_data(self):

        # Recover list of dictionaries from string
        datas = ast.literal_eval(self.data)

        output = list()
        for data in datas:
            # Create empty class with obj_name as name, super with DatabaseObject and data as dictionary
            inst = type(self.object_name, ((DatabaseObject),), data)

            # Define methods to add in this instance
            def get_id(self) -> str:
                return self._id
            def get_timestamp(self) -> int:
                return self._timestamp if hasattr(self, '_timestamp') else None

            # Assign method get_id to this instance
            inst.get_id = types.MethodType(get_id, inst)
            inst.get_timestamp = types.MethodType(get_timestamp, inst)

            # Add to output
            output.append(inst)

        return output


class DatabaseObjectException(Exception):

    def __init__(self, msg: str = '') -> None:
        Exception.__init__(self, msg)


class ErrorMessages(object):

    CONNECTION_ERROR = 'Could not connect to database '
    GET_ERROR = 'Error getting data '
    PUT_ERROR = 'Error storing data '
    UPDATE_ERROR = 'Error updating data '
    REMOVE_ERROR = 'Error removing data '
    CONFIGURATION_ERROR = 'Error configuring database '
    KEYFILE_ERROR = 'Not found the key '
    ID_ERROR = 'Error in the ID format '
    SCHEMA_ERROR = 'Error accessing non-existent schema '
    CRITERIA_ERROR = 'Error in action criteria '
    REPR_ERROR = 'Method __repr__ must be implemented '
    INHERITANCE_ERROR = 'Data must inherit from DatabaseObject '
