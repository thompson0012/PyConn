from pyconn.ops.sync.base import BaseSyncDBClient
from pyconn.utils.db_utils import substitute_sql, SqlResolver
from pyconn.utils.validator import validate_all_true


class UpsertDBSyncClient(BaseSyncDBClient):
    """
    primary key must be present in database for the batch upsert
    """

    def __init__(self, source_client=None, target_client=None):
        super(UpsertDBSyncClient, self).__init__(source_client, target_client)
        self._can_upsert_sync = False

    def _validate_upsert_sync(self):
        validate_all_true([self._extract_sql, self._load_sql])
        self._can_upsert_sync = True

    def sync(self, batch_size):
        self._validate_upsert_sync()
        validate_all_true([self._can_upsert_sync])
        job_count = 0
        q = self.run_extract_sql()
        while True:

            rows = q.fetchmany(batch_size)
            if not bool(rows):
                break
            resolver = SqlResolver()
            serialized_rows = resolver.serialize(rows)
            resolved_rows = resolver.rewrite(serialized_rows)

            sub_sql = substitute_sql(self._load_sql,
                                     resolved_rows)
            self._target_client.execute(sub_sql, True, True)
            job_count += 1

        print('total batch:', job_count)
        return
