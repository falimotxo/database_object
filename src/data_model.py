class DatabaseObject(object):

    def __init__(self, _id: str = '') -> None:
        super(DatabaseObject, self).__init__()
        self._id = _id

    def get_id(self) -> str:
        return self._id

    def set_id(self, _id: str = '') -> None:
        self._id = _id


class DatabaseObjectResult(object):

    def __init__(self, code: str, data: str, msg: str = '', exception: Exception = None) -> None:
        super(DatabaseObjectResult, self).__init__()
        self.code_list = ('OK', 'KO')
        self.code = code
        self.data = data
        self.msg = msg
        self.exception = exception


class DatabaseObjectException(Exception):

    def __init__(self, msg: str = '') -> None:
        super(DatabaseObjectException, self).__init__(msg)


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

    def __init__(self) -> None:
        super(ErrorMessages, self).__init__()
