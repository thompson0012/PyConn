from pyconn.client.db.base import BaseDBClient
from pyconn.ops.sync.base import BaseSyncDBClient
from pyconn.utils.validator import validate_all_true
from pyconn.utils.db_utils import substitute_sql, SqlJoiner, SqlTypeAdapter, SqlRewriter
import re


class FullDBSyncClient(BaseSyncDBClient):
    """
    full database sync, required to have dropped / create statement for table manipulation
    """

    def __init__(self, source_client=None, target_client=None, encode='stringify'):
        super(FullDBSyncClient, self).__init__(source_client, target_client, encode)
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
        self.get_target_client().execute(self._drop_sql, keep_alive=True, commit=True)
        self.get_target_client().execute(self._create_sql, keep_alive=True, commit=True)
        while True:

            rows = q.fetchmany(batch_size)
            if not bool(rows):
                break

            rows = self._type_adapter.parse(rows)
            resolved_rows = SqlJoiner().join(rows, self._encode)

            rewriter = SqlRewriter()
            rewriter.register_rewrite_mapper("(?<![\w\d]){placeholder}(?![\w\d])", resolved_rows)
            rewriter.register_rewrite_mapper(*list(self._NULL_REPLACE)[0])
            rewrote_sql = rewriter.rewrite(self._load_sql)

            self._target_client.execute(rewrote_sql, True, True)
            job_count += 1

        return
