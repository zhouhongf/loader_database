# 从MySQL中提取数据至ElasticSearch, 并制作词云图
from datetime import datetime
from database.backends import MySQLDatabase
from database.backends.elastic_database import update_wealth, update_text
from config import Logger, Config, BankDict
from bs4 import BeautifulSoup
import pickle
import re
from utils.nlp_util import jieba_textrank
from utils.time_util import get_current_week
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from PIL import Image
import cv2
import numpy as np
import os
import time


class DataLoaderMySQL:
    name = 'DataLoaderMySQL'
    logger = Logger().logger
    base_dir = Config.BASE_DIR
    pattern_chinese = re.compile(r'[\u4E00-\u9FA5]+')
    bank_names = [one for one in BankDict.list_bank_alia.keys()]

    def __init__(self):
        self.stopwords = os.path.join(self.base_dir, 'assets/nlp/stopword_cn.txt')
        self.dictwords = os.path.join(self.base_dir, 'assets/nlp/bankdict.txt')
        self.idfwords = os.path.join(self.base_dir, 'assets/nlp/idf.txt.big')
        self.fontpath = os.path.join(self.base_dir, 'assets/nlp/simhei.ttf')

        self.mysqldb = MySQLDatabase()
        self.table_wealth_name = 'wealth'
        self.table_text_name = 'text'
        self.table_wordcloud = 'text_wordcloud'

    def start(self):
        print('【========================== 启动：%s ============================】' % self.name)
        start_time = datetime.now()
        self.dump_wealth_elasticsearch()
        time.sleep(2)
        self.dump_text_elasticsearch()
        time.sleep(2)
        self.make_text_wordcloud()
        end_time = datetime.now()
        print('----------- 用时：%s ------------' % (end_time - start_time))

    def dump_wealth_elasticsearch(self):
        # sql = 'SELECT * FROM %s WHERE %s !="%s"' % (self.table_wealth_name, 'status', 'elastic')
        sql = 'SELECT * FROM %s' % self.table_wealth_name
        results = self.mysqldb.query(sql)
        if results:
            dataList = []
            # 将MySQL的id转为_id
            for one in results:
                one_id = one['id']
                one.pop('id')
                one['_id'] = one_id
                dataList.append(one)

            update_wealth(dataList)
            self.update_data_status(self.table_wealth_name, dataList)

    def dump_text_elasticsearch(self):
        # sql = 'SELECT * FROM %s WHERE %s !="%s"' % (self.table_text_name, 'status', 'elastic')
        sql = 'SELECT * FROM %s' % self.table_text_name
        results = self.mysqldb.query(sql)
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
                # 将MySQL的id转为_id
                data = {
                    '_id': one['id'],
                    'bank_name': one['bank_name'],
                    'name': one['name'],
                    'type_main': one['type_main'],
                    'content': content_need,
                    'create_time': one['create_time']
                }
                dataListElastic.append(data)

            update_text(dataListElastic)
            self.update_data_status(self.table_text_name, dataList)

    def update_data_status(self, table_name: str, dataList: list):
        for one in dataList:
            one['status'] = 'elastic'
            if '_id' in one.keys():
                data_id = one['_id']
                one.pop('_id')
                one['id'] = data_id
            else:
                data_id = one['id']
            self.mysqldb.table_update(table_name=table_name, updates=one, field_where='id', value_where=data_id)

    def make_text_wordcloud(self):
        for bank_name in self.bank_names:
            print('开始制作 %s 的词云图' % bank_name)
            monday, sunday = get_current_week()
            mondayStr = monday.strftime("%Y-%m-%d %H:%M:%S")
            sundayStr = sunday.strftime("%Y-%m-%d %H:%M:%S")
            # sql = 'SELECT * FROM %s WHERE %s ="%s" AND %s >="%s"' % (self.table_text_name, 'bank_name', bank_name, 'create_time', mondayStr)
            sql = 'SELECT * FROM %s WHERE %s ="%s"' % (self.table_text_name, 'bank_name', bank_name)
            res = self.mysqldb.query(sql)
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
                data = {'id': bank_name, 'image': bytes_image}
                d = self.mysqldb.table_has(table_name=self.table_wordcloud, field='id', value=bank_name)
                if d:
                    self.mysqldb.table_update(table_name=self.table_wordcloud, updates=data, field_where='id', value_where=bank_name)
                else:
                    self.mysqldb.table_insert(table_name=self.table_wordcloud, item=data)

                # plt.imshow(wc, interpolation='bilinear')
                # plt.axis('off')
                # plt.savefig('temp.jpg', dpi=200)
                # plt.show()


def start():
    DataLoaderMySQL().start()

