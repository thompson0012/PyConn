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


class AsyncPostgresSQLClient(PostgresSQLClient):
    def __init__(self, db_params):
        super(AsyncPostgresSQLClient, self).__init__(db_params)
