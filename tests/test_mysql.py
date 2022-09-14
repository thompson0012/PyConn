import unittest
import pymysql
from pyconn.client.db.mysql import MySQLClient


class MySQLClientTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)  # add assertion here

    def setUp(self) -> None:
        self._client = MySQLClient.from_kv(host='localhost', port=3306, user='thompson', password='Adminyoov1',
                                           database='yoov')
        self._client.connect()

    def test_execute(self):
        assert len(self._client.execute('select * from test1', keep_alive=True).fetchall()) >= 0, 'fail'

    def test_show_table_schema(self):
        assert len(list(self._client.show_table_schema('test1'))) >= 0, 'fail'

    def test_show_table_ddl(self):
        assert len(list(self._client.show_table_ddl('test1'))) >= 0, 'fail'


if __name__ == '__main__':
    unittest.main()
