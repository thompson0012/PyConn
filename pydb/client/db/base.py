from abc import ABC
from sqlalchemy import create_engine
from addict import Addict


class BaseDBClient(ABC):
    def __init__(self, db_params):
        self._db_params = db_params
        self._conn = None
        self._cursor = None

    def get_db_params(self):
        return self._db_params

    @classmethod
    def from_db_params(cls, db_type, host, user, password, port, db, **kwargs):
        db_params = Addict()
        db_params.db_type = db_type
        db_params.host = host
        db_params.user = user
        db_params.password = password
        db_params.port = port
        db_params.db = db
        db_params.update(**kwargs)

        return cls(db_params.to_dict())

    @classmethod
    def from_raw_params(cls, **kwargs):
        db_params = Addict()
        db_params.update(**kwargs)
        return cls(db_params.to_dict())

    def connect(self):
        raise NotImplementedError

    def execute(self, sql):
        raise NotImplementedError
