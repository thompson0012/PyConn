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
    def from_raw_params(cls, **kwargs):
        lake_params = Addict()
        lake_params.update(**kwargs)
        return cls(lake_params.to_dict())

    def get_lake_params(self):
        return self._lake_params

    def connect(self):
        raise NotImplementedError
