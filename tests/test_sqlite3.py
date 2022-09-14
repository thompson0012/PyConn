import unittest
from pyconn.client.db.sqlite import SQLiteClient
import sqlglot


class SQLiteClientTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)  # add assertion here

    def setUp(self) -> None:
        self._client = SQLiteClient.from_kv(database='./test.db')
        self._client.connect()

    def test_show_table_schema(self):
        assert len(list(self._client.show_table_schema('test_tbl'))) >= 0, 'fail'

    def test_show_table_ddl(self):
        assert len(list(self._client.show_table_ddl("test_tbl"))) >= 0, 'fail'


if __name__ == '__main__':
    unittest.main()
