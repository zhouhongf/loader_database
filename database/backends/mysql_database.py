import time
import logging
import traceback
import pymysql
import pymysql.cursors
from config import Config
from .mongo_database import MongoDatabase

version = "0.1"
version_info = (0, 1, 0, 0)


class MySQLDatabase(object):
    """A lightweight wrapper around PyMySQL.only for python3"""

    # ===================================初始化数据库并建立连接=========================================
    # 一般只需要四个参数就可以建立连接了：
    # •	host：数据库地址，本节就是localhost
    # •	database： 数据库名
    # •	user： 数据库用户名
    # •	password：数据库用户的密码
    # 后面还有几个参数可酌情使用：
    # •	max_idle_time： MySQL server默认8小时闲置就会断开客户端的连接；这个参数告诉客户端闲置多长时间要重新连接；
    # •	time_zone: 这里默认时区为0区，你可以设置为自己的时区，比如东8区 +8:00;
    # •	charset：默认为utf8mb4，即支持moji字符的utf8;

    MYSQLDB = Config.MYSQL_DICT

    def __init__(self, host=None, database=None, user=None, password=None,
                 port=3306,
                 max_idle_time=7 * 3600, connect_timeout=10,
                 time_zone="+0:00", charset="utf8", sql_mode="TRADITIONAL"):
        self.host = host or self.MYSQLDB['host']
        self.database = database or self.MYSQLDB['db']
        self.max_idle_time = float(max_idle_time)

        args = dict(use_unicode=True, charset=charset,
                    database=database or self.MYSQLDB['db'],
                    init_command=('SET time_zone = "%s"' % time_zone),
                    cursorclass=pymysql.cursors.DictCursor,
                    connect_timeout=connect_timeout, sql_mode=sql_mode)
        args["user"] = user or self.MYSQLDB['user']
        args["passwd"] = password or self.MYSQLDB['password']

        # We accept a path to a MySQL socket file or a host(:port) string
        if "/" in self.host:
            args["unix_socket"] = self.host
        else:
            self.socket = None
            pair = self.host.split(":")
            if len(pair) == 2:
                args["host"] = pair[0]
                args["port"] = int(pair[1])
            else:
                args["host"] = self.host
                args["port"] = 3306
        if port:
            args['port'] = port or self.MYSQLDB['port']

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except Exception:
            logging.error("Cannot connect to MySQL on %s", self.host, exc_info=True)

    def _ensure_connected(self):
        # Mysql by default closes client connections that are idle for
        # 8 hours, but the client library does not report this fact until
        # you try to perform a query and it fails.  Protect against this
        # case by preemptively closing and reopening the connection
        # if it has been idle for too long (7 hours by default).
        if self._db is None or (time.time() - self._last_use_time > self.max_idle_time):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        return self._db.cursor()

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()
        self._db = pymysql.connect(**self._db_args)
        self._db.autocommit(True)
        # print('MySQL已连接上，参数是：%s' % self._db_args)

    # ===========================数据库普通操作，查询，插入=========================================
    # 使用query()来获取一组数据，返回的是一个list，其中每个item就是一个dict，跟get()返回的字典一样。
    def query(self, query, *parameters, **kwparameters):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            cursor.execute(query, kwparameters or parameters)
            result = cursor.fetchall()
            return result
        finally:
            cursor.close()

    # 使用get()获取一个数据，返回的是一个dict，key就是数据库表的字段；
    def get(self, query, *parameters, **kwparameters):
        """Returns the (singular) row returned by the given query."""
        cursor = self._cursor()
        try:
            cursor.execute(query, kwparameters or parameters)
            return cursor.fetchone()
        finally:
            cursor.close()

    def execute(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        cursor = self._cursor()
        try:
            cursor.execute(query, kwparameters or parameters)
            return cursor.lastrowid
        except Exception as e:
            if e.args[0] == 1062:
                pass
            else:
                traceback.print_exc()
                raise e
        finally:
            cursor.close()

    # 使用insert()或execute()，看源码就知道，insert就是execute的别名。
    insert = execute

    ## =============== high level method for table ===================
    # table_has() 查询某个值是否存在于表中。查询的字段最好建立的在MySQL中建立了索引，不然数据量稍大就会很慢。
    def table_has(self, table_name, field, value):
        if isinstance(value, str):
            value = value.encode('utf8')

        sql = 'SELECT %s FROM %s WHERE %s="%s"' % (field, table_name, field, value)
        d = self.get(sql)
        return d

    # table_insert() 把一个字典类型的数据插入表中。字典的key必须是表的字段。
    def table_insert(self, table_name, item):
        """item is a dict : key is mysql table field"""
        fields = list(item.keys())
        values = list(item.values())

        fieldstr = ','.join(fields)
        valstr = ','.join(['%s'] * len(item))
        for i in range(len(values)):
            if isinstance(values[i], str):
                values[i] = values[i].encode('utf8')

        sql = 'INSERT INTO %s (%s) VALUES(%s)' % (table_name, fieldstr, valstr)
        try:
            last_id = self.execute(sql, *values)
            return last_id
        except Exception as e:
            if e.args[0] == 1062:
                # just skip duplicated item
                pass
            else:
                traceback.print_exc()
                print('sql:', sql)
                print('item:')
                for i in range(len(fields)):
                    vs = str(values[i])
                    if len(vs) > 300:
                        print(fields[i], ' : ', len(vs), type(values[i]))
                    else:
                        print(fields[i], ' : ', vs, type(values[i]))
                raise e

    # table_update() 更新表中的一条记录。其中, field_where最好是建立了索引，不然数据量稍大就会很慢。
    def table_update(self, table_name, updates, field_where, value_where):
        """updates is a dict of {field_update:value_update}"""
        upsets = []
        values = []
        for k, v in updates.items():
            s = '%s=%%s' % k
            upsets.append(s)
            values.append(v)
        upsets = ','.join(upsets)
        sql = 'UPDATE %s SET %s WHERE %s="%s"' % (
            table_name,
            upsets,
            field_where, value_where,
        )
        self.execute(sql, *values)


def update_mysql_from_mongo(table_name: str, dataList: list):
    time_start = time.perf_counter()
    print('================= 运行MySQL更新%s: %s =================' % (table_name, time_start))
    mysqldb = MySQLDatabase()

    for data in dataList:
        data_id = data['_id']
        data.pop('_id')
        data['id'] = data_id

        d = mysqldb.table_has(table_name=table_name, field='id', value=data_id)
        if d:
            mysqldb.table_update(table_name=table_name, updates=data, field_where='id', value_where=data_id)
        else:
            mysqldb.table_insert(table_name=table_name, item=data)

    time_end = time.perf_counter()
    print('=============== 完成MySQL更新时间: %s, 用时: %s ==============' % (time_end, (time_end - time_start)))


def update_text_wordcloud_from_mongo():
    mysqldb = MySQLDatabase()
    table_name = 'text_wordcloud'

    mongo_db = MongoDatabase().db()
    collection_text_wordcloud = mongo_db[table_name]
    wordclouds = collection_text_wordcloud.find()
    if wordclouds:
        for data in wordclouds:
            data_id = data['_id']
            data.pop('_id')
            data['id'] = data_id
            # print('id是：%s' % data_id)
            d = mysqldb.table_has(table_name=table_name, field='id', value=data_id)
            if d:
                mysqldb.table_update(table_name=table_name, updates=data, field_where='id', value_where=data_id)
            else:
                mysqldb.table_insert(table_name=table_name, item=data)
