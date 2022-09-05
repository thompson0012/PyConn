import unittest
from sqlalchemy import create_engine
import toml
from addict import Addict
from pydb.client.base import BaseDBClient


class MyTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)  # add assertion here

    def setUp(self) -> None:
        self.config = Addict(toml.load('.toml'))

    def test_db_connect(self):
        db_client = BaseDBClient.from_db_user(db_type='mysql', db_driver='pymysql', host=self.config.db.url, port=self.config.db.port, user=self.config.db.user, password=self.config.db.password, db='yoov_oa_new')
        db_client._db_engine.connect()

if __name__ == '__main__':
    unittest.main()
