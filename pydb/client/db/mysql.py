from pydb.client.db.base import BaseDBClient
import aiomysql
import pymysql


class MySQLClient(BaseDBClient):
    def __init__(self, db_params):
        super(MySQLClient, self).__init__(db_params)

    def connect(self):
        conn = pymysql.connect(self.get_db_params())
        self._conn = conn
        self._cursor = conn.cursor()
        return self

    def execute(self, sql):
        try:
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
