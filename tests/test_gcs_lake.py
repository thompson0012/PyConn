import unittest
from pydb.client.lake.gcs import GCSClient
import json
import pathlib


class GCSClientTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)  # add assertion here

    def setUp(self) -> None:
        credentials = json.load(open('../gcs-dev@yoov-dev.iam.gserviceaccount.com.json', 'r'))
        self.client = GCSClient.from_kv(bucket='yoov-data-team-dev', credentials=credentials)
        self.client.connect()

    def test_upload_files(self):
        self.client.execute('upload', 'pyproject-dev/pydb-connect/test_gcs_lake.py', method='filename',
                            filename=pathlib.Path.cwd() / 'test_gcs_lake.py')

    def test_download_files(self):
        self.client.execute('download', 'pyproject-dev/pydb-connect/test_gcs_lake.py', method='local',filename='t.py')


if __name__ == '__main__':
    unittest.main()
