from pydb.ops2.sync.base import BaseSync


class FullSync(BaseSync):
    def __init__(self, client):
        super(FullSync, self).__init__(client)
