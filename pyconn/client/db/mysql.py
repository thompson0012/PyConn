from pyconn.client.db.base import BaseDBClient
import aiomysql
import pymysql
from typing import List
from pyconn.utils.db_utils import tuple_to_dict


class MySQLClient(BaseDBClient):
    def __init__(self, db_params):
        super(MySQLClient, self).__init__(db_params)

    def connect(self):
        conn = pymysql.connect(**self.get_db_params())
        self._conn = conn
        self._cursor = conn.cursor()
        return self

    def execute(self, sql, keep_alive=False):
        if keep_alive:
            q = self._cursor.execute(sql)
            # pymysql after execute, will return an amount of rows that are affected,
            # can't use fetchall to retrieval data directly
            return self.get_cursor()
        try:
            self._cursor.execute(sql)
            self._conn.commit()

        except Exception as e:
            print('failed', e)
            self._conn.rollback()

        finally:
            self.close_conn()

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

    def show_table_schema(self, tbl_name):
        data = self.execute(f'describe {tbl_name}', keep_alive=True).fetchall()
        return map(lambda x: tuple_to_dict(x, ['field', 'type', 'null', 'key', 'default', 'extra']), data)

    def show_table_ddl(self, tbl_name):
        data = self.execute(f'show create table {tbl_name}', keep_alive=True).fetchall()
        return map(lambda x: tuple_to_dict(x, ['table', 'sql']), data)


class AsyncMySQLClient(MySQLClient):
    def __init__(self, db_params):
        super(AsyncMySQLClient, self).__init__(db_params)
