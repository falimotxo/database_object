import time

from common import config
from common.infra_exception import InfraException
from database_object_module.database_object_module import DatabaseObjectModule
from database_object_module.data_model import DatabaseObject

module_names = {
    'database_object_module': DatabaseObjectModule
}

modules_manager = dict()


class DatabaseObjectTest1(DatabaseObject):

    def __init__(self) -> None:
        DatabaseObject.__init__(self)
        self.value = 1
        self.bool_arg_2 = bool(False)

    def __repr__(self):
        return str(self.__dict__)

    def __str__(self):
        return str(self.__dict__)


def main():
    logger = config.get_log('infra')

    logger.info('INIT MODULES')

    # Read config and build all active modules
    section_modules = config.get_sections()
    for section_module in section_modules:
        try:
            active = config.get_value(section_module, 'active') == 'True'
        except InfraException:
            active = False

        if active:
            # Create an instance and store in dictionary
            module = module_names[section_module](config)
            modules_manager[section_module] = module

    logger.info('ALL MODULES STARTED')

    # time.sleep(5)
    #
    # dom = modules_manager['database_object_module']
    # o = DatabaseObjectTest1()
    # object_name = o.__class__.__name__
    # ret = dom.put_object('SCHEMA', object_name, o)

    while True:
        time.sleep(2)


if __name__ == "__main__":
    main()
