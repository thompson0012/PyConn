from pyconn.client.db.base import BaseDBClient
import asyncio
import asyncpg
import psycopg
from typing import List
import flashtext


class PostgresSQLClient(BaseDBClient):
    def __init__(self, db_params):
        super(PostgresSQLClient, self).__init__(db_params)

    def connect(self):
        conn = psycopg.connect(**self.get_db_params())
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


class AsyncPostgresSQLClient(PostgresSQLClient):
    def __init__(self, db_params):
        super(AsyncPostgresSQLClient, self).__init__(db_params)
