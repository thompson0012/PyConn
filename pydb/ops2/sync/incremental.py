from pydb.ops2.sync.base import BaseSync


class IncrementalSync(BaseSync):
    def __init__(self, client):
        super(IncrementalSync, self).__init__(client)

    def get_incremental_records(self, statement):
        incremental_records = self.get_records(statement)
        return incremental_records

    def get_updated_records(self, statement):
        updated_records = self.get_records(statement)
        return updated_records

    def sync_to(self, target):
        pass
