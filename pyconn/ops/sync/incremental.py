from pyconn.ops.sync.base import BaseSyncDBClient
from pyconn.client.db.base import BaseDBClient
from typing import List
from pyconn.utils.db_utils import substitute_sql
from pyconn.utils.validator import validate_all_true


class IncrementalDBSyncClient(BaseSyncDBClient):
    def __init__(self, source_client=None, target_client=None):
        super(IncrementalDBSyncClient, self).__init__(source_client, target_client)
        self._can_incremental_sync = False

    def _validate_incremental_sync(self):
        validate_all_true([self._extract_sql, self._load_sql])
        self._can_incremental_sync = True

    def sync(self, batch_size):
        validate_all_true([self._can_incremental_sync])
        job_count = 0
        q = self.run_extract_sql()
        while True:

            print(job_count)
            rows = q.fetchmany(batch_size)
            if not bool(rows):
                break
            sub_sql = substitute_sql(self._load_sql,
                                     rows)
            self._target_client.execute(sub_sql, True, True)
            job_count += 1
        return
