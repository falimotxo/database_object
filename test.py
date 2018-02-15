import os
import logging
import logging.config

def setup_logger(logger_name, log_file, level=logging.INFO):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(message)s')
    fileHandler = logging.FileHandler(log_file, mode='w')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)

    l.setLevel(level)
    l.addHandler(fileHandler)
    l.addHandler(streamHandler)


# txtName = 'zzzzz'
#
# setup_logger('log1', txtName+".txt")
# setup_logger('log2', txtName+"small.txt")
# logger_1 = logging.getLogger('log1')
# logger_2 = logging.getLogger('log2')

# logger_1.info('111messasage 1')
# logger_2.info('222ersaror foo')


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


# Path configuration of file
CONFIGURE_FILE = 'config.ini'
LOGGING_FILE = 'logging.ini'

# Common folders
BASE_PATH = os.path.dirname(__file__)
CONFIG_FOLDER = 'etc'
CONFIG_PATH = BASE_PATH + '/{}/'.format(CONFIG_FOLDER)
CONFIG_LOGGING_FULL_PATH = CONFIG_PATH + LOGGING_FILE
CONFIG_FILE_FULL_PATH = CONFIG_PATH + CONFIGURE_FILE

logging.config.fileConfig(CONFIG_LOGGING_FULL_PATH)
for h in logging.root.handlers:
    h.setFormatter(CustomFormatter(h.formatter))

log1 = logging.getLogger('database_object_module')
log2 = logging.getLogger('infraxxx')

log1.info('log1log1')

log2.info('log2log2')
log2.info('22222')
log2.info('33333')
log2.info('44444')
