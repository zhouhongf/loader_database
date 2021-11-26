import os


class Config:

    GROUP_NAME = 'ubank'
    PROJECT_NAME = 'loader_database'

    SCHEDULED_DICT = {
        'time_interval': int(os.getenv('TIME_INTERVAL', 1440)),             # 定时时间间隔24小时
    }

    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    ROOT_DIR = os.path.dirname(BASE_DIR)
    LOAD_DIR = os.path.join(ROOT_DIR, 'dataload')
    os.makedirs(LOAD_DIR, exist_ok=True)
    LOG_DIR = os.path.join(BASE_DIR, 'logs')
    os.makedirs(LOG_DIR, exist_ok=True)

    HOST_LOCAL = '192.168.50.172'
    # HOST_LOCAL = '192.168.3.110'
    MONGO_DICT = {
        'host': HOST_LOCAL,
        'port': 27017,
        'db': GROUP_NAME,
        'username': 'root',
        'password': 'Zhouhf873@',
    }

    MYSQL_DICT = {
        'host': HOST_LOCAL,
        'port': 3306,
        'db': GROUP_NAME,
        'user': 'root',
        'password': '20110919Zyy==20170215Zyy',
    }
