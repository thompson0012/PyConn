from pyconn.client.db.base import BaseDBClient
from google.cloud import bigquery
from google.cloud.bigquery.dbapi import Connection, Cursor
from pyconn.utils.validator import validate_keys
from pyconn.utils.db_utils import tuple_to_dict


class BigQueryClient(BaseDBClient):
    def __init__(self, db_params=None):
        """

        Args:
            db_params: dict
            {'client_params':{'project':'test_project',
            'location':'asia-east2a'}}
        """
        super(BigQueryClient, self).__init__(db_params)
        self._client = None

    def connect(self) -> "BaseDBClient":
        validate_keys(self.get_db_params(), require=['client_params'])
        validate_keys(self.get_db_params('client_params'), require=['project'])
        client = bigquery.Client(**self.get_db_params('client_params'))
        self._client = client
        self._conn = Connection(client)
        self._cursor: Cursor = self._conn.cursor()
        return self

    def get_client(self):
        return self._client

    def execute(self, sql: str, params=None, *args, **kwargs):
        try:
            if params is not None:
                self._cursor.execute(sql, params, *args, **kwargs)
                self._conn.commit()
            else:
                self._cursor.execute(sql, *args, **kwargs)
                self._conn.commit()

        except Exception as e:
            print('failed', e)

        return self._cursor

    def show_table_ddl(self, tbl_name, dataset_name):
        return self.execute(
            'select ddl from `{0}`.INFORMATION_SCHEMA.TABLES where table_name = "{1}"'.format(dataset_name,
                                                                                              tbl_name)).fetchall()

    def show_table_schema(self, tbl_name, dataset_name):
        rows = self.execute(
            'select table_name, column_name, data_type from `{0}`.INFORMATION_SCHEMA.COLUMNS where table_name="{1}"'.format(
                dataset_name, tbl_name)).fetchall()

        return list(map(lambda x: tuple_to_dict(x, ['table_name','column_name','data_type']), map(lambda x: x.values(), rows)))
