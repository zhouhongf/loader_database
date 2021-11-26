import time
from config import singleton, Logger
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from datetime import datetime
from numpy import long

log = Logger().logger


@singleton
class ElasticDatabase:

    def __init__(self):
        # 无用户名密码状态
        # self.es = Elasticsearch([ip])
        # self.es = Elasticsearch(hosts='root:password@192.168.50.172:9200')
        self.es = Elasticsearch(hosts='192.168.50.172:9200')
        # 用户名密码状态
        # self.es = Elasticsearch([ip], http_auth=('elastic', 'password'), port=9200)

    def generate_bulk_datas_with_ids(self, index_name, datas):
        for data in datas:
            yield {'_index': index_name, '_id': data['_id'], '_source': data}

    def generate_bulk_datas_and_ids(self, index_name, datas):
        for idx, da in enumerate(datas):
            idx += 1
            yield {'_index': index_name, '_id': idx, '_source': da}

    # 如果不存在此index, 则新建
    def create_index_with_mappings(self, index_name, mappings):
        if not self.es.indices.exists(index=index_name):
            res = self.es.indices.create(index=index_name, ignore=400)
            print(res)
            res_map = self.es.indices.put_mapping(index=index_name, body=mappings)
            print(res_map)

    def delete_index(self, index_name):
        if self.es.indices.exists(index=index_name):
            res = self.es.indices.delete(index=index_name, ignore=[400, 404])
            print(res)

    def insert_one_data_with_id(self, index_name, data):
        self.es.index(index=index_name, id=data['_id'], body=data)

    def insert_one_data_and_id(self, index_name, data):
        self.es.index(index=index_name, body=data)

    def bulk_data_with_id(self, index_name, datas):
        bulk(self.es, self.generate_bulk_datas_with_ids(index_name, datas))

    def bulk_data_and_id(self, index_name, datas):
        bulk(self.es, self.generate_bulk_datas_and_ids(index_name, datas))

    def search_all(self, index_name):
        return self.es.search(index=index_name)

    def delete_one_data(self, index_name, id):
        res = self.es.delete(index=index_name, id=id)
        print(res)

    def query_keywords(self, index_name: str, query_target: str, keywords: list):
        query_content = ''
        for word in keywords:
            query_content += (word + ' ')
        query_content = query_content[:-1]
        dsl = {'query': {'match': {query_target: query_content}}}
        return self.es.search(index=index_name, body=dsl)

    def query_multi_keywords(self, index_name, keyOne, valueOne, keyTwo, valueTwo):
        dsl = {'query': {'bool': {'must': [
            {'match': {keyOne: valueOne}},
            {'match': {keyTwo: valueTwo}}
        ]}}}
        return self.es.search(index=index_name, body=dsl)

    def query_multi_match(self, index_name, keyOne, keyTwo, value):
        query = {'query': {'multi_match': {'query': value, 'fields': [keyOne, keyTwo]}}}
        return self.es.search(index=index_name, body=query)

    # 参考https://techoverflow.net/?s=ElasticSearch
    def es_iterate_all_documents(self, index_name, pagesize=250, scroll_timeout="1m", **kwargs):
        """
        Helper to iterate ALL values from a single index
        Yields all the documents.
        """
        global scroll_id
        is_first = True
        while True:
            # Scroll next
            if is_first:  # Initialize scroll
                result = self.es.search(index=index_name, scroll="1m", **kwargs, body={"size": pagesize})
                is_first = False
            else:
                result = self.es.scroll(body={"scroll_id": scroll_id, "scroll": scroll_timeout})

            scroll_id = result["_scroll_id"]
            hits = result["hits"]["hits"]
            # Stop after no more docs
            if not hits:
                break
            # Yield each entry
            yield from (hit['_source'] for hit in hits)


def update_wealth(dataList: list):
    time_start = time.perf_counter()
    print('================= 运行ElasticDB更新WEALTH: %s =================' % time_start)

    elastic = ElasticDatabase()
    index_name = 'wealth'
    mappings = {
        'dynamic': False,
        'properties': {
            'name': {
                'type': 'text',
                'analyzer': 'ik_max_word',
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            'bank_name': {
                'type': 'text',
                'analyzer': 'ik_max_word',
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            'code': {'type': 'keyword'},
            'code_register': {'type': 'keyword'},
            'create_time': {'type': 'date'}
        }
    }
    elastic.create_index_with_mappings(index_name, mappings)
    for data in dataList:
        _id = data['_id']
        if not elastic.es.exists(index=index_name, id=_id):
            local_datetime = datetime.strptime(data['create_time'], "%Y-%m-%d %H:%M:%S")
            create_time = long(time.mktime(local_datetime.timetuple()) * 1000.0 + local_datetime.microsecond / 1000.0)
            dataIn = {
                'name': data['name'],
                'bank_name': data['bank_name'],
                'code': data['code'],
                'code_register': data['code_register'],
                'create_time': create_time
            }
            elastic.es.index(index=index_name, id=_id, body=dataIn)

    time_end = time.perf_counter()
    print('=============== 完成ElasticDB更新WEALTH: %s, 用时: %s ==============' % (time_end, (time_end - time_start)))


def update_text(dataList: list):
    time_start = time.perf_counter()
    print('================= 运行ElasticDB更新TEXT: %s =================' % time_start)

    elastic = ElasticDatabase()
    index_name = 'news'
    mappings = {
        'dynamic': False,
        'properties': {
            'name': {
                'type': 'text',
                'analyzer': 'ik_max_word',
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            'bank_name': {
                'type': 'text',
                'analyzer': 'ik_max_word',
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            'content': {
                'type': 'text',
                'analyzer': 'ik_max_word',
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            'type_main': {'type': 'keyword'},
            'create_time': {'type': 'date'}
        }
    }
    elastic.create_index_with_mappings(index_name, mappings)
    for data in dataList:
        _id = data['_id']
        if not elastic.es.exists(index=index_name, id=_id):
            local_datetime = datetime.strptime(data['create_time'], "%Y-%m-%d %H:%M:%S")
            create_time = long(time.mktime(local_datetime.timetuple()) * 1000.0 + local_datetime.microsecond / 1000.0)
            dataIn = {
                'name': data['name'],
                'bank_name': data['bank_name'],
                'content': data['content'],
                'type_main': data['type_main'],
                'create_time': create_time
            }
            elastic.es.index(index=index_name, id=_id, body=dataIn)

    time_end = time.perf_counter()
    print('=============== 完成ElasticDB更新TEXT: %s, 用时: %s ==============' % (time_end, (time_end - time_start)))
