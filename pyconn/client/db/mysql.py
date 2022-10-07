from pyconn.client.db.base import BaseDBClient
import aiomysql
import pymysql
from typing import List, Callable
from pyconn.utils.db_utils import tuple_to_dict, SqlTypeAdapter
from pyconn.utils.validator import validate_opts_value, validate_opts_type


class MySQLClient(BaseDBClient):
    def __init__(self, db_params):
        super(MySQLClient, self).__init__(db_params)

    def connect(self):
        conn = pymysql.connect(**self.get_db_params())
        self._conn = conn
        self._cursor = conn.cursor()
        return self

    def show_table_schema(self, tbl_name):
        data = self.execute(f'describe {tbl_name}').fetchall()
        return map(lambda x: tuple_to_dict(x, ['field', 'type', 'null', 'key', 'default', 'extra']), data)

    def show_table_ddl(self, tbl_name):
        data = self.execute(f'show create table {tbl_name}').fetchall()
        return map(lambda x: tuple_to_dict(x, ['table', 'sql']), data)


class AsyncMySQLClient(MySQLClient):
    def __init__(self, db_params):
        super(AsyncMySQLClient, self).__init__(db_params)

    