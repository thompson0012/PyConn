from pydb.client.db.base import BaseDBClient
import asyncio
import asyncpg


class PostgresSQLClient(BaseDBClient):
    def __init__(self, db_params):
        super(PostgresSQLClient, self).__init__(db_params)
