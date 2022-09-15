from pyconn.client.db.base import BaseDBClient
from pyconn.ops.sync.base import BaseSyncClient


class FullSyncClient(BaseSyncClient):
    def __init__(self, source_client=None, target_client=None):
        super(FullSyncClient, self).__init__(source_client, target_client)

