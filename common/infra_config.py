import configparser
import logging
import logging.config
import logging.handlers
import os
import logstash

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

    HOST_LOGSTASH = None
    PORT_LOGSTASH = None
    FORMATTER = None
    MAX_SIZE = 104857600
    MAX_FILES = 10
    ATTR_LOG_LEVEL = 'log_level'
    ATTR_LOG_FORMATTER = 'log_formatter'
    ATTR_HOST_LOGSTASH = 'log_logstash_host'
    ATTR_PORT_LOGSTASH = 'log_logstash_port'

    # Path configuration of file
    CONFIGURE_FILE = 'config.ini'

    # Common folders
    BASE_PATH = os.path.dirname(__file__)
    CONFIG_FOLDER = 'etc'
    CONFIG_PATH = BASE_PATH + '/../{}/'.format(CONFIG_FOLDER)
    CONFIG_FILE_FULL_PATH = CONFIG_PATH + CONFIGURE_FILE

    def __init__(self) -> None:
        """
        Constructor: read properties file
        """

        # Logging config
        # logging.config.fileConfig(InfraConfig.CONFIG_LOGGING_FULL_PATH)
        # for h in logging.root.handlers:
        #     h.setFormatter(CustomFormatter(h.formatter))

        # Config file
        self.config = configparser.RawConfigParser()
        self.config.read(InfraConfig.CONFIG_FILE_FULL_PATH)

        InfraConfig.FORMATTER = self.get_value('infra', InfraConfig.ATTR_LOG_FORMATTER)

        # If not exists logstash configuration, do anything
        try:
            InfraConfig.HOST_LOGSTASH  = self.get_value('infra', InfraConfig.ATTR_HOST_LOGSTASH)
            InfraConfig.PORT_LOGSTASH  = self.get_value('infra', InfraConfig.ATTR_PORT_LOGSTASH)
        except:
            pass

        self.logs = dict()


        section_modules = self.get_sections()
        for section_module in section_modules:
            log = self.setup_logger(section_module, self.get_value(section_module, InfraConfig.ATTR_LOG_LEVEL))
            self.logs[section_module] = log

    def get_log(self, logger_name):
        if logger_name in self.logs.keys():
            return self.logs[logger_name]
        else:
            raise InfraException(GenericErrorMessages.UNKNOWN_LOG_ERROR)

    def setup_logger(self, logger_name, level):

        # Create logger
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)

        # Create formatter
        original_formatter = logging.Formatter(InfraConfig.FORMATTER)
        f = CustomFormatter(original_formatter)

        # Create rotating file handler
        rfh = logging.handlers.RotatingFileHandler('../{}.log'.format(logger_name), 'a',
                                                   InfraConfig.MAX_SIZE, InfraConfig.MAX_FILES)
        rfh.setLevel(level)
        rfh.setFormatter(f)

        # Create console handler
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(f)

        # Create logstash handler
        if InfraConfig.HOST_LOGSTASH is not None and InfraConfig.PORT_LOGSTASH is not None:
            lh = logstash.TCPLogstashHandler(InfraConfig.HOST_LOGSTASH, InfraConfig.PORT_LOGSTASH, version=1)
            logger.addHandler(lh)

        # Add the handlers to the logger
        logger.addHandler(rfh)
        logger.addHandler(ch)
        logger.propagate = 0

        return logger

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
        except Exception as e:
            raise InfraException(GenericErrorMessages.KEYFILE_ERROR)
