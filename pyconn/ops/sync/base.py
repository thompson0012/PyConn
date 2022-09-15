from sqlalchemy.engine import Engine
from pyconn.client.db.base import BaseDBClient


class BaseSync:
    def __init__(self, db_client: BaseDBClient):
        self._db_client = db_client

    def get_records(self, statement):
        return self.get_conn().execute(statement)

    def get_conn(self):
        return self._db_client.get_conn()

    def get_cursor(self):
        return self._db_client.get_cursor()

    def sync_to(self, target_db_client: BaseDBClient, sql: str):
        pass


class BaseSyncClient:
    def __init__(self, source_client=None, target_client=None):
        self._source_client: BaseDBClient = source_client
        self._target_client: BaseDBClient = target_client

        self._extract_sql = None
        self._load_sql = None

    def register_source(self, client: BaseDBClient):
        self._source_client = client
        return

    def register_target(self, client: BaseDBClient):
        self._target_client = client
        return

    def connect_all(self):
        self._target_client.connect()
        self._source_client.connect()
        return

    def register_extract_sql(self, sql):
        self._extract_sql = sql
        return

    def register_load_sql(self, sql):
        self._load_sql = sql

    def batch_job(self, batch_size):
        while self._source_client.execute(self._extract_sql, True, True).fetchmany(batch_size):
            self._target_client.execute(self._load_sql, True, True)

        return
