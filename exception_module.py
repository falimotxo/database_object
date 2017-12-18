class ExceptionModule(Exception):
    pass

class ErrorMessages(object):
    CONNECTION_ERROR = 'Could not connect to database'
    GET_ERROR = 'Error getting data'
    PUT_ERROR = 'Error storing data'
    UPDATE_ERROR = 'Error updating data'
    REMOVE_ERROR = 'Error removing data'

