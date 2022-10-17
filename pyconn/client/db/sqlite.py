from pyconn.client.db.base import BaseDBClient
import sqlite3
from typing import List, Callable
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

    def show_table_schema(self, tbl_name):
        data = self.execute(f'pragma table_info({tbl_name})').fetchall()
        return map(lambda x: tuple_to_dict(x, ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk']), data)

    def show_table_ddl(self, tbl_name):
        data = self.execute(f'select * from sqlite_schema where tbl_name="{tbl_name}"').fetchall()
        return map(lambda x: tuple_to_dict(x, ['type', 'name', 'tbl_name', 'rootpage', 'sql']), data)
