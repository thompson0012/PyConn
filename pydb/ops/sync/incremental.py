from pydb.ops.sync.base import BaseSync
from pydb.client.db.base import BaseDBClient


class IncrementalSync(BaseSync):
    def __init__(self, db_client: BaseDBClient):
        super(IncrementalSync, self).__init__(db_client)

    def get_incremental_records(self, statement):
        incremental_records = self.get_records(statement)
        return incremental_records

    def get_updated_records(self, statement):
        updated_records = self.get_records(statement)
        return updated_records

    def sync_to(self, target_db_client, sql):
        conn = target_db_client.connect()
        conn.execute(sql)

