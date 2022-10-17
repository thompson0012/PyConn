from abc import ABC
from addict import Addict


class BaseLakeClient(ABC):
    def __init__(self, lake_params: dict):
        self._lake_params: dict = lake_params
        self._client = None
        self._conn = None

    @classmethod
    def from_lake_params(cls):
        raise NotImplementedError

    @classmethod
    def from_kv(cls, **kwargs):
        lake_params = Addict()
        lake_params.update(**kwargs)
        return cls(lake_params.to_dict())

    def get_lake_params(self, k=None):
        if k:
            return self._lake_params.get(k, None)
        return self._lake_params

    def connect(self):
        raise NotImplementedError

    def upload(self, destination_name, method, **kwargs):
        raise NotImplementedError

    def download(self, destination_name, method, **kwargs):
        raise NotImplementedError

    def execute(self, download_or_upload, destination_name, method, **kwargs):
        match download_or_upload:
            case 'download':
                return self.download(destination_name, method, **kwargs)
            case 'upload':
                return self.upload(destination_name, method, **kwargs)
            case _:
                raise KeyError('only support [download, upload]')

    def get_conn(self):
        return self._conn

    def get_meta_data(self):
        raise NotImplementedError
