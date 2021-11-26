from pymongo import MongoClient, collection
from config import Config, singleton, Logger
import time
import pickle
import farmhash
import os
import re

MONGODB = Config.MONGO_DICT
log = Logger().logger


@singleton
class MongoDatabase:

    def client(self):
        mongo = MongoClient(
            host=MONGODB['host'] if MONGODB['host'] else 'localhost',
            port=MONGODB['port'] if MONGODB['port'] else 27017,
            username=MONGODB['username'] if MONGODB['username'] else '',
            password=MONGODB['password'],
        )
        return mongo

    def db(self):
        return self.client()[MONGODB['db']]

    @staticmethod
    def upsert(collec: collection, condition: dict, data: dict):
        result = collec.find_one(condition)
        if result:
            collec.update_one(condition, {'$set': data})
            print('MONGO数据库《%s》中upsert更新: %s' % (collec.name, condition))
            return None
        else:
            collec.insert_one(data)
            print('MONGO数据库《%s》中upsert新增: %s' % (collec.name, condition))
            return condition

    @staticmethod
    def do_insert_one(collec: collection, condition: dict, data: dict):
        result = collec.find_one(condition)
        if result:
            print('MONGO数据库《%s》中do_insert_one已存在: %s' % (collec.name, condition))
            return None
        else:
            collec.insert_one(data)
            print('MONGO数据库《%s》中do_insert_one新增: %s' % (collec.name, condition))
            return condition


def load_data_pickle(database: str, collection_name: str, filepath: str = Config.LOAD_DIR):
    log.info('================================== 开始执行load_data_pickle ======================================')
    time_start = time.perf_counter()

    mongo = MongoDatabase()
    mongo_db = mongo.client()
    db_target = mongo_db[database]
    collection_target = db_target[collection_name]

    filename = os.path.join(filepath, collection_name + '.pkl')
    with open(filename, 'rb') as f:
        list_data = pickle.load(f)

    for data in list_data:
        id = data['_id']                                # 检查id是否全部为数字格式
        result = re.compile(r'\d+').fullmatch(id)
        if not result:
            id = str(farmhash.hash64(id))
            data['_id'] = id

        if 'term' in data.keys():
            term = data['term']                             # 检查期限，期限当中，可能存在‘无固定期限’或‘-’等格式
            res = re.compile(r'\d+').fullmatch(str(term))
            if not res:
                data['term'] = None

        if collection_name == 'TEXT':
            data['content'] = pickle.loads(data['content'])
        mongo.do_insert_one(collection_target, {'_id': data['_id']}, data)

    time_used = time.perf_counter() - time_start
    log.info('====================结束load_data_pickle, 用时：%s========================' % time_used)
