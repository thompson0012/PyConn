from pyconn.ops.sync.base import BaseSyncDBClient
from pyconn.client.db.base import BaseDBClient
from typing import List


class IncrementalDBSyncClient(BaseSyncDBClient):
    def __init__(self, source_client=None, target_client=None):
        super(IncrementalDBSyncClient, self).__init__(source_client, target_client)

    