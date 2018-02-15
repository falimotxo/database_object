import configparser
import logging
import logging.config
import os

from common.infra_exception import InfraException, GenericErrorMessages


class CustomFormatter(object):
    """
    Custom formatter to solve issues with decorators
    """

    def __init__(self, orig_formatter):
        self.orig_formatter = orig_formatter

    def format(self, record):
        if hasattr(record, 'name_override'):
            record.funcName = record.name_override
        if hasattr(record, 'file_override'):
            record.filename = record.file_override
        if hasattr(record, 'lineno_override'):
            record.lineno = record.lineno_override

        msg = self.orig_formatter.format(record)
        return msg


class InfraConfig(object):
    """
    Class to get the configuration options
    """

    # Path configuration of file
    CONFIGURE_FILE = 'config.ini'
    LOGGING_FILE = 'logging.ini'

    # Common folders
    BASE_PATH = os.path.dirname(__file__)
    CONFIG_FOLDER = 'etc'
    CONFIG_PATH = BASE_PATH + '/../{}/'.format(CONFIG_FOLDER)
    CONFIG_LOGGING_FULL_PATH = CONFIG_PATH + LOGGING_FILE
    CONFIG_FILE_FULL_PATH = CONFIG_PATH + CONFIGURE_FILE

    def __init__(self) -> None:
        """
        Constructor: read properties file
        """

        # Logging config
        logging.config.fileConfig(InfraConfig.CONFIG_LOGGING_FULL_PATH)
        for h in logging.root.handlers:
            h.setFormatter(CustomFormatter(h.formatter))

        # Config file
        self.config = configparser.ConfigParser()
        self.config.read(InfraConfig.CONFIG_FILE_FULL_PATH)

    def get_sections(self) -> list:
        """
        Recover sections of config

        :return: sections of config
        :rtype: list
        """

        return self.config.sections()

    def get_value(self, section: str, key: str) -> str:
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
        except Exception:
            raise InfraException(GenericErrorMessages.KEYFILE_ERROR)
