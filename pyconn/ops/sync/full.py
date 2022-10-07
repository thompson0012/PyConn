from pyconn.ops.sync.base import BaseSyncDBClient
from pyconn.utils.validator import validate_all_true


class FullDBSyncClient(BaseSyncDBClient):
    """
    full database sync, required to have dropped / create statement for table manipulation
    """

    def __init__(self, source_client=None, target_client=None):
        super(FullDBSyncClient, self).__init__(source_client, target_client)
        self._drop_sql = None
        self._create_sql = None

    def register_drop_sql(self, sql):
        self._drop_sql = sql
        return

    def register_create_sql(self, sql):
        self._create_sql = sql
        return

    def sync(self, batch_size):
        validate_all_true([self._extract_sql, self._load_sql, self._create_sql, self._drop_sql])
        job_count = 0
        q = self.run_extract_sql()
        self.get_target_client().execute(self._drop_sql)
        self.get_target_client().execute(self._create_sql)
        while True:

            rows = q.fetchmany(batch_size)
            if not bool(rows):
                break

            self._target_client.executemany(self._load_sql, rows)
            job_count += 1
        self.disconnect_all()
        return
