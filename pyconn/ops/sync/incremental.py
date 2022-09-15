from pyconn.ops.sync.base import BaseSyncClient
from pyconn.client.db.base import BaseDBClient
from typing import List


class IncrementalSync(BaseSyncClient):
    def __init__(self, source_client=None, target_client=None):
        super(IncrementalSync, self).__init__(source_client, target_client)
