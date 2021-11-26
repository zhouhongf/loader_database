# 从MongoDB中提取数据至 ElasticSearch和MySQL中
from datetime import datetime
from pymongo import collection
from database.backends import MongoDatabase
from database.backends.elastic_database import update_wealth, update_text
from database.backends.mysql_database import update_mysql_from_mongo, update_text_wordcloud_from_mongo
from config import Logger, Config, BankDict
from bs4 import BeautifulSoup
import pickle
import re
from utils.nlp_util import jieba_textrank
from utils.time_util import get_current_week
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import jieba
from PIL import Image
import cv2
import numpy as np
import os
import time


class DataLoader:
    name = 'DataLoader'
    logger = Logger().logger
    base_dir = Config.BASE_DIR
    pattern_chinese = re.compile(r'[\u4E00-\u9FA5]+')
    bank_names = [one for one in BankDict.list_bank_alia.keys()]

    def __init__(self):
        self.mongo = MongoDatabase()
        mongo_db = self.mongo.db()
        self.collection_manual = mongo_db['MANUAL']
        self.collection_wealth = mongo_db['WEALTH']
        self.collection_text = mongo_db['TEXT']
        self.collection_text_wordcloud = mongo_db['text_wordcloud']
        self.stopwords = os.path.join(self.base_dir, 'assets/nlp/stopword_cn.txt')
        self.dictwords = os.path.join(self.base_dir, 'assets/nlp/bankdict.txt')
        self.idfwords = os.path.join(self.base_dir, 'assets/nlp/idf.txt.big')
        self.fontpath = os.path.join(self.base_dir, 'assets/nlp/simhei.ttf')

    def start(self):
        print('【========================== 启动：%s ============================】' % self.name)
        start_time = datetime.now()
        self.dump_manual_mysql()
        time.sleep(2)
        self.dump_wealth_elasticsearch()
        time.sleep(2)
        self.dump_text_elasticsearch()
        time.sleep(2)
        self.make_text_wordcloud()
        time.sleep(2)
        update_text_wordcloud_from_mongo()
        end_time = datetime.now()
        print('----------- 用时：%s ------------' % (end_time - start_time))

    # 使用$sample随机取一定数量的数据，防止数据量多大，载入内存，造成服务死掉
    def dump_manual_mysql(self):
        select_command = [{'$match': {'status': {'$ne': 'mysql'}}}, {'$sample': {'size': 50}}]
        # results = self.collection_manual.aggregate(select_command)
        results = self.collection_manual.find()
        if results:
            dataList = [one for one in results]
            update_mysql_from_mongo(table_name='manual', dataList=dataList)
            self.update_data_status(self.collection_manual, dataList, 'mysql')

    def dump_wealth_elasticsearch(self):
        select_command = [{'$match': {'status': {'$ne': 'elastic'}}}, {'$sample': {'size': 500}}]
        # results = self.collection_wealth.aggregate(select_command)
        results = self.collection_wealth.find()
        if results:
            dataList = [one for one in results]
            update_wealth(dataList)
            self.update_data_status(self.collection_wealth, dataList, 'elastic')
            update_mysql_from_mongo(table_name='wealth', dataList=dataList)

    def dump_text_elasticsearch(self):
        select_command = [{'$match': {'status': {'$ne': 'elastic'}}}, {'$sample': {'size': 100}}]
        # results = self.collection_text.aggregate(select_command)
        results = self.collection_text.find()
        if results:
            dataList = []
            dataListElastic = []
            for one in results:
                content = one['content']
                if isinstance(content, bytes):
                    content = pickle.loads(content)
                one['content'] = content
                dataList.append(one)

                soup = BeautifulSoup(content, 'lxml')
                text = soup.get_text(strip=True)
                text = re.sub(r'\s+', '', text)
                content_need = text
                if len(text) > 100:
                    content_need = text[:100] + '...'
                data = {
                    '_id': one['_id'],
                    'bank_name': one['bank_name'],
                    'name': one['name'],
                    'type_main': one['type_main'],
                    'content': content_need,
                    'create_time': one['create_time']
                }
                dataListElastic.append(data)

            update_text(dataListElastic)
            update_mysql_from_mongo(table_name='text', dataList=dataList)
            self.update_data_status(self.collection_text, dataList, 'elastic')

    @staticmethod
    def update_data_status(collec: collection, dataList: list, status: str):
        for one in dataList:
            one['status'] = status
            if '_id' in one.keys():
                collec.update_one({'_id': one['_id']}, {'$set': one})
            elif 'id' in one.keys():
                collec.update_one({'_id': one['id']}, {'$set': one})

    def make_text_wordcloud(self):
        for bank_name in self.bank_names:
            print('开始制作 %s 的词云图' % bank_name)
            monday, sunday = get_current_week()
            mondayStr = monday.strftime("%Y-%m-%d %H:%M:%S")
            sundayStr = sunday.strftime("%Y-%m-%d %H:%M:%S")
            # res = self.collection_text.find({'$and': [{'bank_name': {'$eq': bank_name}}, {'create_time': {'$gte': mondayStr}}]})
            res = self.collection_text.find({'bank_name': {'$eq': bank_name}})
            keywords = dict()
            for one in res:
                content = one['content']
                results = self.pattern_chinese.findall(content)
                if results:
                    results_join = ' '.join(results)
                    keywords_dict = jieba_textrank(results_join, self.dictwords, self.idfwords, self.stopwords)
                    keywords.update(keywords_dict)
            if keywords:
                wc = WordCloud(font_path=self.fontpath, width=400, height=300, mode='RGBA', background_color=None, random_state=1).generate_from_frequencies(keywords)
                wc_array = wc.to_array()
                r, buf = cv2.imencode(".png", wc_array)
                bytes_image = Image.fromarray(np.uint8(buf)).tobytes()
                data = {'_id': bank_name, 'image': bytes_image}
                self.collection_text_wordcloud.update_one({'_id': data['_id']}, {'$set': data}, upsert=True)

                # plt.imshow(wc, interpolation='bilinear')
                # plt.axis('off')
                # plt.savefig('temp.jpg', dpi=200)
                # plt.show()


def start():
    DataLoader().start()

