from sqlalchemy.engine import Engine
from pydb.client.db.base import BaseDBClient


class BaseSync:
    def __init__(self, db_client: BaseDBClient):
        self._db_client = db_client

    def get_records(self, statement):
        return self.get_conn().execute(statement)

    def get_conn(self):
        return self._db_client.get_conn()

    def get_cursor(self):
        return self._db_client.get_cursor()

    def sync_to(self,  target_db_client: BaseDBClient, sql: str):
        pass
