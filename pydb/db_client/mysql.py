from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from pydb.db_client.base import BaseDBClient


class MySQLClient(BaseDBClient):
    def __init__(self, engine):
        super(MySQLClient, self).__init__(engine)
