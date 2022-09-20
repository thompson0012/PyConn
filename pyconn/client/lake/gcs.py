from pyconn.client.lake.base import BaseLakeClient
from google.oauth2 import service_account
from pyconn.utils.validator import validate_keys
from google.cloud.storage import Client, Bucket, Blob


class GCSClient(BaseLakeClient):

    def __init__(self, lake_params):
        """

        Args:
            lake_params: dict
            {"bucket":"abc",
            "credentials":{"type":'',
                            "project_id":'',
                            "private_key_id":'',
                            "private_key":'',
                            "client_email":'',
                            "client_id":'',
                            "auth_uri":'',
                            "token_uri":'',
                            "auth_provider_x509_cert_url":'',
                            "client_x509_cert_url":'',
                            }}
        """
        super(GCSClient, self).__init__(lake_params)
        self._conn: Bucket

    @classmethod
    def from_credential_files(cls, lake_params: dict):
        import os
        import json
        path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        s = open(path)
        credentials = json.loads(s)
        lake_params.update({'credentials': credentials})
        return

    def connect(self):
        validate_keys(self.get_lake_params(), require=['bucket', 'credentials'])
        validate_keys(self.get_lake_params().get('credentials', {}),
                      require=["type", "project_id", "private_key_id", "private_key", "client_email", "client_id",
                               "auth_uri", "token_uri", "auth_provider_x509_cert_url", "client_x509_cert_url"])
        credential = service_account.Credentials.from_service_account_info(
            self.get_lake_params().get('credentials', {}))
        self._client = Client(credentials=credential)
        self._conn = self._client.bucket(self.get_lake_params().get('bucket', {}))

    def upload(self, destination_name, method, **kwargs):
        self._conn: Bucket
        blob = self._conn.blob(destination_name)

        controller = GCSFileUploadController(blob, destination_name)
        q = controller.redirect(method)(**kwargs)
        return q

    def download(self, destination_name, method, **kwargs):
        blob = self._conn.blob(destination_name)

        controller = GCSFileDownloadController(blob, destination_name)
        q = controller.redirect(method)(**kwargs)
        return q

    def get_meta_data(self):
        self._conn: Bucket
        from pyconn.model.meta_data import MetaDataModel
        meta_data = MetaDataModel()
        meta_data.add_meta_data('id', self._conn.id)
        meta_data.add_meta_data('name', self._conn.name)
        meta_data.add_meta_data('storage_class', self._conn.storage_class)
        meta_data.add_meta_data('localtion', self._conn.location)
        meta_data.add_meta_data('location_type', self._conn.location_type)
        meta_data.add_meta_data('cors', self._conn.cors)
        meta_data.add_meta_data('default_event_based_hold', self._conn.default_event_based_hold)
        meta_data.add_meta_data('default_kms_key_name', self._conn.default_kms_key_name)
        meta_data.add_meta_data('metageneration', self._conn.metageneration)
        meta_data.add_meta_data('public_access_prevention', self._conn.iam_configuration)
        meta_data.add_meta_data('retention_effective_time', self._conn.retention_policy_effective_time)
        meta_data.add_meta_data('retention_period', self._conn.retention_period)
        meta_data.add_meta_data('requester_pays', self._conn.requester_pays)
        meta_data.add_meta_data('self_link', self._conn.self_link)
        meta_data.add_meta_data('time_created', self._conn.time_created)
        meta_data.add_meta_data('versioning_enabled', self._conn.versioning_enabled)
        meta_data.add_meta_data('labels', self._conn.labels)
        return meta_data.to_dict()


class GCSFileController:
    def __init__(self, blob, destination):
        self._blob: Blob = blob
        self._destination = destination

    def redirect(self, method):
        raise NotImplementedError


class GCSFileUploadController(GCSFileController):
    def __init__(self, blob, destination):
        super(GCSFileUploadController, self).__init__(blob, destination)

    def redirect(self, method: str):
        match method:
            case 'file':
                return self._blob.upload_from_file

            case 'filename':
                return self._blob.upload_from_filename

            case 'string':
                return self._blob.upload_from_string

            case _:
                raise KeyError('only support [file, filename, string]')


class GCSFileDownloadController(GCSFileController):
    def __init__(self, blob, destination):
        super(GCSFileDownloadController, self).__init__(blob, destination)

    def redirect(self, method):
        match method:
            case 'local':
                return self._blob.download_to_filename
            case 'bytes':
                return self._blob.download_as_bytes
            case 'text':
                return self._blob.download_as_text
            case 'string':
                return self._blob.download_as_string
            case _:
                raise KeyError('only support [local, bytes, text, string]')
