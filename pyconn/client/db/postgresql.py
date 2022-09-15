from pyconn.client.db.base import BaseDBClient
import asyncio
import asyncpg
import psycopg
from typing import List
import humre
from pyconn.utils.validator import validate_opts_value


class PostgresSQLClient(BaseDBClient):
    def __init__(self, db_params):
        super(PostgresSQLClient, self).__init__(db_params)

    def connect(self):
        conn = psycopg.connect(**self.get_db_params())
        self._conn = conn
        self._cursor = conn.cursor()
        return self

    def execute(self, sql, keep_alive=False, commit=True):
        # should add auto-infer sql action
        # read = False
        # compiler = humre.compile("(?<![\w\d])create|insert|update|delete|drop|alter(?![\w\d])", IGNORECASE=True)
        # if len(compiler.findall(sql)) == 0:
        #     read = True
        if keep_alive:
            if commit:
                q = self._cursor.execute(sql)
                self._conn.commit()
                return q
            q = self._cursor.execute(sql)
            return q

        validate_opts_value(commit, True)
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
            self.close_conn()


class AsyncPostgresSQLClient(PostgresSQLClient):
    def __init__(self, db_params):
        super(AsyncPostgresSQLClient, self).__init__(db_params)
