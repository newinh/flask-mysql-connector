import logging
import threading
from contextlib import contextmanager

import sqlparse
from mysql.connector.connection import MySQLConnection
from mysql.connector.cursor import CursorBase
from mysql.connector.pooling import MySQLConnectionPool

from flask import Flask

logger = logging.getLogger(__name__)


class MySQL(object):
    mysql_host = '127.0.0.1'
    mysql_user = 'root'
    mysql_password = '1234'
    mysql_db = 'db'
    mysql_pool_size = 10

    _connection_pool = None
    __singleton_lock = threading.Lock()

    def __init__(self):
        if MySQL._connection_pool:
            return

        with MySQL.__singleton_lock:
            if not MySQL._connection_pool:
                MySQL._connection_pool = MySQLConnectionPool(host=MySQL.mysql_host,
                                                             user=MySQL.mysql_user,
                                                             password=MySQL.mysql_password,
                                                             db=MySQL.mysql_db,
                                                             charset='utf8',
                                                             pool_name="db_pool",
                                                             pool_size=MySQL.mysql_pool_size,
                                                             buffered=True)

    @classmethod
    def init_app(cls, app: Flask):
        cls.mysql_host = app.config['MYSQL_HOST']
        cls.mysql_user = app.config['MYSQL_USER']
        cls.mysql_password = app.config['MYSQL_PASSWORD']
        cls.mysql_db = app.config['MYSQL_DB']
        cls.mysql_pool_size = app.config['MYSQL_POOL_SIZE']

    @classmethod
    @contextmanager
    def cursor(cls, buffered=None, raw=None, prepared=None, cursor_class=None,
               dictionary=None, named_tuple=None, pool=True) -> CursorBase:

        conn = cls._connection_pool.get_connection() if pool else MySQLConnection(host=cls.mysql_host,
                                                                                  user=cls.mysql_user,
                                                                                  password=cls.mysql_password,
                                                                                  db=cls.mysql_db,
                                                                                  charset='utf8',
                                                                                  buffered=True)

        cursor = conn.cursor(buffered=buffered, raw=raw,
                             prepared=prepared, cursor_class=cursor_class,
                             dictionary=dictionary, named_tuple=named_tuple)
        try:
            yield cursor
        except Exception as e:
            conn.rollback()
            if cursor.statement:
                logger.error(sqlparse.format(cursor.statement, reindent=True))
            logger.error(e)
            raise e
        finally:
            conn.commit()
            cursor.close()
            conn.close()
