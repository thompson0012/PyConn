from pydb.ops.sync.base import BaseSync
from pydb.client.db.base import BaseDBClient


class FullSync(BaseSync):
    def __init__(self, db_client: BaseDBClient):
        super(FullSync, self).__init__(db_client)

    def sync_to(self,  target_db_client: BaseDBClient, sql: str):
        conn = target_db_client.connect()
        conn.execute()