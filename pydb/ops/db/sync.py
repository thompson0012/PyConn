from pydb.ops.db.base import DBOperator
from sqlalchemy.engine import Engine, Connection


class BaseSync(DBOperator):
    def __init__(self, client):
        super(BaseSync, self).__init__(client)

    def get_records(self, statement):
        return self.get_conn().execute(statement)

    def sync_to(self, target: Engine):
        pass


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


class FullSync(BaseSync):
    def __init__(self, client):
        super(FullSync, self).__init__(client)

