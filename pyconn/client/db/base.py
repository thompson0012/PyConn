from abc import ABC
from sqlalchemy import create_engine
from addict import Addict
from typing import List


class BaseDBClient(ABC):
    def __init__(self, db_params):
        self._db_params = db_params
        self._conn = None
        self._cursor = None

    def get_db_params(self):
        return self._db_params

    @classmethod
    def from_db_params(cls, db_type, host, user, password, port, db, **kwargs):
        db_params = Addict()
        db_params.db_type = db_type
        db_params.host = host
        db_params.user = user
        db_params.password = password
        db_params.port = port
        db_params.db = db
        db_params.update(**kwargs)

        return cls(db_params.to_dict())

    @classmethod
    def from_kv(cls, **kwargs):
        db_params = Addict()
        db_params.update(**kwargs)
        return cls(db_params.to_dict())

    def connect(self) -> "BaseDBClient":
        raise NotImplementedError

    def reconnect(self):
        return self.connect()

    def execute(self, sql, keep_alive: bool):
        raise NotImplementedError

    def execute_many(self, sql_ls: List[str]):
        raise NotImplementedError

    def get_conn(self):
        return self._conn

    def get_cursor(self):
        return self._cursor

    def show_table_schema(self, tbl_name):
        raise NotImplementedError

    def show_table_ddl(self, tbl_name):
        raise NotImplementedError

    def close_conn(self):
        self._cursor.close()
        self._conn.close()
        return
