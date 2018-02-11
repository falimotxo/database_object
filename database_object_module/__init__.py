import logging
from database_object_module.database_object_config import DatabaseConfigModule

# Read config and set logger
config = DatabaseConfigModule()
logger = logging.getLogger(__name__)