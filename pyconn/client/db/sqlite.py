from pyconn.client.db.base import BaseDBClient
import sqlite3
from typing import List
from pyconn.utils.db_utils import tuple_to_dict
from pyconn.utils.validator import validate_opts_value


class SQLiteClient(BaseDBClient):
    def __init__(self, db_params):
        super(SQLiteClient, self).__init__(db_params)

    def connect(self):
        conn = sqlite3.connect(**self.get_db_params())
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
            q = self._cursor.execute(sql)
            if commit:
                self._conn.commit()
            return self._cursor

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

    def show_table_schema(self, tbl_name):
        data = self.execute(f'pragma table_info({tbl_name})', keep_alive=True).fetchall()
        return map(lambda x: tuple_to_dict(x, ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk']), data)

    def show_table_ddl(self, tbl_name):
        data = self.execute(f'select * from sqlite_schema where tbl_name="{tbl_name}"', keep_alive=True).fetchall()
        return map(lambda x: tuple_to_dict(x, ['type', 'name', 'tbl_name', 'rootpage', 'sql']), data)
