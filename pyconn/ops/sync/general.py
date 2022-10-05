import re
from pyconn.ops.sync.base import BaseSyncDBClient
from pyconn.utils.db_utils import substitute_sql, SqlJoiner, SqlRewriter
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

            rows = self._type_adapter.parse(rows)
            resolved_rows = SqlJoiner().join(rows, self._encode)

            rewriter = SqlRewriter()
            rewriter.register_rewrite_mapper("(?<![\w\d]){{values}}(?![\w\d])", resolved_rows)
            rewriter.register_rewrite_mapper(*list(self._NULL_REPLACE.items())[0])
            rewrote_sql = rewriter.rewrite(self._load_sql)

            self._target_client.execute(rewrote_sql, True, True)
            job_count += 1

        return
