from pyconn.client.db.base import BaseDBClient
from pyconn.utils.db_utils import SqlTypeAdapter
from typing import Optional


class BaseSyncDBClient:
    JSONIFY_NULL = {'"null"': 'null'}
    STRINGIFY_NULL = {"'null'": 'null'}

    def __init__(self, source_client=None, target_client=None, encode='stringify'):
        self._source_client: BaseDBClient = source_client
        self._target_client: BaseDBClient = target_client
        self._encode = encode
        self._NULL_REPLACE = self.STRINGIFY_NULL if self._encode == 'stringify' else self.JSONIFY_NULL
        self._type_adapter: Optional[SqlTypeAdapter] = None

        self._extract_sql = None
        self._load_sql = None
        self._transform_func = None

    def register_source(self, client: BaseDBClient):
        self._source_client = client
        return

    def register_target(self, client: BaseDBClient):
        self._target_client = client
        return

    def register_type_adapter(self, adapter: SqlTypeAdapter):
        self._type_adapter = adapter
        return

    def connect_all(self):
        self._target_client.connect()
        self._source_client.connect()
        return

    def close_all(self):
        self._source_client.disconnect()
        self._target_client.disconnect()
        return

    def register_extract_sql(self, sql):
        self._extract_sql = sql
        return

    def register_load_sql(self, sql):
        self._load_sql = sql
        return

    def register_transform_func(self, partial_func):
        self._transform_func = partial_func
        return

    def get_source_client(self):
        return self._source_client

    def get_target_client(self):
        return self._target_client

    def run_extract_sql(self):
        q = self._source_client.execute(self._extract_sql, True, True)
        return q

    def sync(self, batch_size):
        raise not NotImplementedError
