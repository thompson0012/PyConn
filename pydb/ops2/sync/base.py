from sqlalchemy.engine import Engine

from pydb.ops.db.base import DBOperator


class BaseSync(DBOperator):
    def __init__(self, client):
        super(BaseSync, self).__init__(client)

    def get_records(self, statement):
        return self.get_conn().execute(statement)

    def sync_to(self, target: Engine):
        pass
