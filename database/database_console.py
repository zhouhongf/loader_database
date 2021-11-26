from importlib import import_module
from config import Config


# 三张表：MANUAL, WEALTH, TEXT
def database_console():
    # mongodb_module = import_module('database.backends.mongo_database')
    # mongodb_module.load_data_pickle(database=Config.GROUP_NAME, collection_name='TEXT')
    # mongodb_module.load_data_pickle(database=Config.GROUP_NAME, collection_name='WEALTH')
    # mongodb_module.load_data_pickle(database=Config.GROUP_NAME, collection_name='MANUAL')
    loader_module = import_module('database.data_loader_mysql')
    loader_module.start()


if __name__ == '__main__':
    database_console()
