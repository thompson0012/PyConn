from pyconn.client.db.base import BaseDBClient, AsyncDBClient
import aiomysql
import pymysql
from pyconn.utils.db_utils import tuple_to_dict, SqlTypeAdapter


class MySQLClient(BaseDBClient):
    def __init__(self, db_params):
        super(MySQLClient, self).__init__(db_params)

    def connect(self):
        conn = pymysql.connect(**self.get_db_params())
        self._conn = conn
        self._cursor = conn.cursor()
        self._cursor.execute()
        return self

    def show_table_schema(self, tbl_name):
        data = self.execute(f'describe {tbl_name}').fetchall()
        return map(lambda x: tuple_to_dict(x, ['field', 'type', 'null', 'key', 'default', 'extra']), data)

    def show_table_ddl(self, tbl_name):
        data = self.execute(f'show create table {tbl_name}').fetchall()
        return map(lambda x: tuple_to_dict(x, ['table', 'sql']), data)


class AsyncMySQLClient(AsyncDBClient, MySQLClient):
    def __init__(self, db_params):
        super(AsyncMySQLClient, self).__init__(db_params=db_params)

    def connect(self):
        async def make_conn():
            conn = await aiomysql.connect(**self._db_params)
            cursor = await conn.cursor()
            return conn, cursor

        conn, cursor = self.get_db_params('loop').run_until_complete(make_conn())
        self._conn = conn
        self._cursor = cursor

    def execute(self, sql, *args, **kwargs):
        async def do_execute():
            try:
                q = await self._cursor.execute(sql, *args, **kwargs)
                await self._conn.commit()
            except Exception as e:
                print('failed:', e)
                self._conn.rollback()

            return self._cursor

        loop: "AbstractEventLoop" = self.get_db_params('loop')
        loop.run_until_complete(do_execute())
        return self._cursor

    def show_table_schema(self, tbl_name):
        data = self.execute(f'describe {tbl_name}').fetchall()
        return map(lambda x: tuple_to_dict(x, ['field', 'type', 'null', 'key', 'default', 'extra']), data.result())

    def show_table_ddl(self, tbl_name):
        data = self.execute(f'show create table {tbl_name}').fetchall()
        return map(lambda x: tuple_to_dict(x, ['table', 'sql']), data.result())
