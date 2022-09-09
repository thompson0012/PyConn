from pydb.client.lake.base import BaseLakeClient
from google.oauth2 import service_account
from pydb.utils.validator import validate_keys
from google.cloud.storage import Client


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

    def connect(self):
        validate_keys(self.get_lake_params().get('credentials', {}),
                      require=["type", "project_id", "private_key_id", "private_key", "client_email", "client_id",
                               "auth_uri", "token_uri", "auth_provider_x509_cert_url", "client_x509_cert_url"])
        credential = service_account.Credentials.from_service_account_info(
            self.get_lake_params().get('credentials', {}))
        self._client = Client(credentials=credential)
        self._conn = self._client.bucket(self.get_lake_params().get('bucket', {}))

    def execute(self):
        pass

