from pydb.client.db.base import BaseDBClient


class MySQLClient(BaseDBClient):
    def __init__(self, engine):
        super(MySQLClient, self).__init__(engine)
