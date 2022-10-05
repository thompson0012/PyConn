from pyconn.ops.sync.base import BaseSyncDBClient
from pyconn.utils.db_utils import substitute_sql, SqlJoiner, SqlRewriter
from pyconn.utils.validator import validate_all_true
import re


class UpsertDBSyncClient(BaseSyncDBClient):
    """
    primary key must be present in database for the batch upsert
    """

    def __init__(self, source_client=None, target_client=None, encode='stringify'):
        super(UpsertDBSyncClient, self).__init__(source_client, target_client, encode)
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

            rows = self._type_adapter.parse(rows)
            resolved_rows = SqlJoiner().join(rows, self._encode)

            rewriter = SqlRewriter()
            rewriter.register_rewrite_mapper("(?<![\w\d]){{values}}(?![\w\d])", resolved_rows)
            rewriter.register_rewrite_mapper(*list(self._NULL_REPLACE.items())[0])
            rewrote_sql = rewriter.rewrite(self._load_sql)
            self._target_client.execute(rewrote_sql, True, True)
            job_count += 1

        return
