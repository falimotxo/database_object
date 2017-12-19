class DatabaseObject(object):

    def __init__(self, id: str = None) -> None:
        self.id = id


class DatabaseObjectResult(object):

    def __init__(self, code: str, data: str, msg: str = '', exception: Exception = None) -> None:
        super().__init__()
        self.code_list = ('OK', 'KO')
        self.code = code
        self.data = data
        self.msg = msg
        self.exception = exception


class DatabaseObjectException(Exception):
    pass


class ErrorMessages(object):
    CONNECTION_ERROR = 'Could not connect to database '
    GET_ERROR = 'Error getting data '
    PUT_ERROR = 'Error storing data '
    UPDATE_ERROR = 'Error updating data '
    REMOVE_ERROR = 'Error removing data '
    CONFIGURATION_ERROR = 'Error configuring database'
    KEYFILE_ERROR = 'Error looking for the key '
