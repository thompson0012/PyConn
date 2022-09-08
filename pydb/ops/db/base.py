"""
read
insert
update
sync
delete
translate
"""
from sqlalchemy.engine import Engine
from sqlglot import transpile
from pydb.ops import BaseOperator
from pydb.client.db.base import BaseDBClient


class BaseDB(BaseOperator):
    def __init__(self, client):
        super(BaseDB, self).__init__(client)


class DBOperator(BaseDB):
    def __init__(self, client):
        super(DBOperator, self).__init__()
        self._client:BaseDBClient = client

    def get_db_engine(self):
        return self._client.get_db_engine()

    def get_conn(self):
        return self._client.get_conn()

    def transpile(self):
        pass
