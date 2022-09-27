from pyconn.client.db.base import BaseDBClient
import aiomysql
import pymysql
from typing import List, Callable
from pyconn.utils.db_utils import tuple_to_dict, SqlTypeConverter
from pyconn.utils.validator import validate_opts_value, validate_opts_type


class MySQLClient(BaseDBClient):
    def __init__(self, db_params):
        super(MySQLClient, self).__init__(db_params)

    def register_adapt(self, value_type, handler_func: Callable):
        validate_opts_type(value_type, int)
        validate_opts_type(handler_func, Callable)

        raise NotImplementedError

    def register_conv(self, value_type, handler_func: Callable):
        validate_opts_type(value_type, int)
        validate_opts_type(handler_func, Callable)

        conversions = self._db_params.get('conv', pymysql.converters.conversions)
        converter = SqlTypeConverter(conversions)
        # conversions[value_type] = handler_func
        converter.register_mapper(value_type, handler_func)
        self._db_params.update({'conv': converter.get_mapper()})
        return

    def init_default_conv(self):
        self.register_conv(pymysql.FIELD_TYPE.DECIMAL, float)
        self.register_conv(pymysql.FIELD_TYPE.DATE, str)
        self.register_conv(pymysql.FIELD_TYPE.TIMESTAMP, str)
        self.register_conv(pymysql.FIELD_TYPE.DATETIME, str)
        self.register_conv(pymysql.FIELD_TYPE.TIME, str)

        return

    def connect(self):
        conn = pymysql.connect(**self.get_db_params())
        self._conn = conn
        self._cursor = conn.cursor()
        return self

    def execute(self, sql, keep_alive=False, commit=True):

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
