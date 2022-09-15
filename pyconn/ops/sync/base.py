from pyconn.client.db.base import BaseDBClient
from pyconn.utils.db_utils import substitute_sql


class BaseSyncClient:
    def __init__(self, source_client=None, target_client=None):
        self._source_client: BaseDBClient = source_client
        self._target_client: BaseDBClient = target_client

        self._extract_sql = None
        self._load_sql = None
        self._transform_func = None

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

    def close_all(self):
        self._source_client.close_conn()
        self._target_client.close_conn()
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

    def sync_job(self, batch_size):
        job_count = 0
        q = self._source_client.execute(self._extract_sql, True, True)
        while True:

            print(job_count)
            rows = q.fetchmany(batch_size)

            if bool(rows):
                break
            sub_sql = substitute_sql(self._load_sql,
                                     rows)
            self._target_client.execute(sub_sql, True, True)
            job_count += 1

        return
