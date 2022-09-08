from abc import ABC
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


class BaseDBClient(ABC):
    def __init__(self, engine):
        self._db_engine:Engine = engine

    @classmethod
    def from_db_user(cls, db_type, db_driver, host, user, password, port, db, charset='UTF-8', echo=True):
        engine = create_engine(f"{db_type}+{db_driver}://{user}:{password}@{host}:{port}/{db}", echo=echo)
        return cls(engine)

    @classmethod
    def from_db_path(cls, url, echo=True):
        engine = create_engine(f"{url}", echo=echo)
        return cls(engine)

    def get_db_engine(self):
        return self._db_engine

    def get_conn(self):
        return self._db_engine.connect()
