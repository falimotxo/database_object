import time

from common import config, logger
from common.infra_exception import InfraException

from database_object_module.database_object_module import DatabaseObjectModule


module_names = {
    'database_object_module': DatabaseObjectModule
}

modules_manager = dict()


def main():
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

    while True:
        time.sleep(2)

if __name__== "__main__":
    main()


