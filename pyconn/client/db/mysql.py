from pyconn.client.db.base import BaseDBClient
import aiomysql
import pymysql
from typing import List
import flashtext


class MySQLClient(BaseDBClient):
    def __init__(self, db_params):
        super(MySQLClient, self).__init__(db_params)

    def connect(self):
        conn = pymysql.connect(**self.get_db_params())
        self._conn = conn
        self._cursor = conn.cursor()
        return self

    def execute(self, sql):
        read = False
        kp = flashtext.KeywordProcessor()
        kp.add_keyword('select')
        if len(kp.extract_keywords(sql)) >= 1:
            read = True
        if read:
            q = self._cursor.execute(sql)
            return q
        try:
            self._cursor.execute(sql)
            self._conn.commit()

        except Exception as e:
            print('failed', e)
            self._conn.rollback()

        finally:
            self._cursor.close()
            self._conn.close()

    def execute_many(self, sql_ls: List[str]):
        try:
            for sql in sql_ls:
                self._cursor.execute(sql)
                self._conn.commit()

        except Exception as e:
            print('failed', e)
            self._conn.rollback()

        finally:
            self._cursor.close()
            self._conn.close()


class AsyncMySQLClient(MySQLClient):
    def __init__(self, db_params):
        super(AsyncMySQLClient, self).__init__(db_params)
