from pyconn.ops.sync.base import BaseSyncDBClient
from pyconn.utils.validator import validate_all_true


class GeneralDBSyncClient(BaseSyncDBClient):
    """
    general class for database sync, not required to drop/create table
    you have to make sure the target table already created and match the schema
    """

    def __init__(self, source_client=None, target_client=None, encode='stringify'):
        super(GeneralDBSyncClient, self).__init__(source_client, target_client, encode)

    def sync(self, batch_size):
        validate_all_true([self._extract_sql, self._load_sql])
        job_count = 0
        q = self.run_extract_sql()
        while True:

            rows = q.fetchmany(batch_size)
            if not bool(rows):
                break

            self._target_client.execute_many(self._load_sql, rows)
            job_count += 1
        self.disconnect_all()
        return
