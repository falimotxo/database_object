

class InfraException(Exception):
    """
    Standard exception
    """

    def __init__(self, msg: str = '') -> None:
        Exception.__init__(self, msg)


class GenericErrorMessages:
    """
    Class of generic standard messages
    """

    # Error messages
    KEYFILE_ERROR = 'Not found the key'