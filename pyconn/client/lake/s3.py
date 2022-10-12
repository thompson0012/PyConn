import boto3
from pyconn.utils.validator import validate_keys

from pyconn.client.lake.base import BaseLakeClient


class S3Client(BaseLakeClient):
    def __init__(self, lake_params):
        """

        Parameters
        ----------
        lake_params: dict, {'bucket_params': {'bucket': 'test'}}
        """
        super(S3Client, self).__init__(lake_params)

    def connect(self):
        validate_keys(self.get_lake_params(), require=['bucket_params'])
        validate_keys(self.get_lake_params('bucket_params'), require=['bucket'])
        self._client = boto3.client('s3')
        self._conn = self._client.Bucket(self.get_lake_params().get('bucket', {}))

    def download(self, destination_name, method, **kwargs):
        controller = S3FileDownloadController(self._conn, destination_name, method, **kwargs)
        q = controller.redirect(method)(**kwargs)
        return q

    def upload(self, destination_name, method, **kwargs):
        controller = S3FileUploadController(self._conn, destination_name, method, **kwargs)
        q = controller.redirect(method)(**kwargs)
        return q

    def get_meta_data(self):
        pass


class S3FileController:
    def __init__(self, bucket, destination):
        self._bucket = bucket
        self._destination = destination

    def redirect(self, method):
        raise NotImplementedError


class S3FileUploadController(S3FileController):
    def __init__(self, bucket, destination):
        super(S3FileUploadController, self).__init__(bucket, destination)

    def redirect(self, method):
        match method:
            case 'file':
                return self._bucket.upload_file

            case 'fileobj':
                return self._bucket.upload_fileobj

            case _:
                raise KeyError('only support [file, fileobj]')


class S3FileDownloadController(S3FileController):
    def __init__(self, bucket, destination):
        super(S3FileDownloadController, self).__init__(bucket, destination)

    def redirect(self, method):
        match method:
            case 'file':
                return self._bucket.download_file

            case 'fileobj':
                return self._bucket.download_fileobj

            case _:
                raise KeyError('only support [file, fileobj]')
