from pyconn.ops.sync.base import BaseSync
from pyconn.client.db.base import BaseDBClient
from typing import List


class IncrementalSync(BaseSync):
    def __init__(self, db_client: BaseDBClient):
        super(IncrementalSync, self).__init__(db_client)

    def get_incremental_records(self, sql):
        incremental_records = self.get_records(sql)
        return incremental_records

    def get_updated_records(self, sql):
        updated_records = self.get_records(sql)
        return updated_records

    def sync_to(self, target_db_client, sqls: List[str]):
        conn = target_db_client.connect()
        conn.execute(sqls)
