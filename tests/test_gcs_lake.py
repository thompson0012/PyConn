import unittest
from pyconn.client.lake.gcs import GCSClient
import json
import pathlib


class GCSClientTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)  # add assertion here

    def setUp(self) -> None:
        credentials = json.load(open('../gcs-dev@yoov-dev.iam.gserviceaccount.com.json', 'r'))
        self.client = GCSClient.from_kv(bucket='yoov-data-team-dev', credentials=credentials)
        self.client.connect()

    def test_upload_local_file(self):
        self.client.execute('upload', 'pyproject-dev/pyconn-connect/test_gcs_lake.py', method='filename',
                            filename=pathlib.Path.cwd() / 'test_gcs_lake.py')

    def test_upload_memory(self):
        import pandas as pd
        df = pd.DataFrame([[123, 123, 123]])
        self.client.execute('upload', 'pyproject-dev/pyconn-connect/test_file.csv', method='string', data=df.to_csv())

    def test_download_files(self):
        self.client.execute('download', 'pyproject-dev/pyconn-connect/test_gcs_lake.py', method='local', filename='test_GCSClient.py')

    def test_download_to_memory(self):
        self.client.execute('download', 'pyproject-dev/pyconn-connect/test_file.csv', method='string')

if __name__ == '__main__':
    unittest.main()
