from pyconn.client.db.base import BaseDBClient
from google.cloud import bigquery
from google.cloud.bigquery.dbapi import Connection, Cursor
from pyconn.utils.validator import validate_keys


class BigQueryClient(BaseDBClient):
    def __init__(self, db_params=None):
        """

        Args:
            db_params: dict
            {'client_params':{'project':'test_project',
            'credentials':Credentials,
            'location':'asia-east2a'}}
        """
        super(BigQueryClient, self).__init__(db_params)

    def connect(self) -> "BaseDBClient":
        validate_keys(self.get_db_params(), require=['client_params'])
        validate_keys(self.get_db_params('client_params'), require=['project'])
        client = bigquery.Client(**self.get_db_params('client_params'))
        self._conn = Connection(client)
        self._cursor: Cursor = self._conn.cursor()
        return self
