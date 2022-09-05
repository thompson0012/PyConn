from abc import ABC
from sqlalchemy import create_engine


class BaseDBClient(ABC):
    def __init__(self, engine):
        self._db_engine = engine

    @classmethod
    def from_db_user(cls, db_type, db_driver, host, user, password, port, db, charset='UTF-8', echo=True):
        engine = create_engine(f"{db_type}+{db_driver}://{user}:{password}@{host}:{port}/{db}", echo=echo)
        return cls(engine)

    @classmethod
    def from_db_path(cls, url, echo=True):
        engine = create_engine(f"{url}", echo=echo)
        return cls(engine)

    def insert(self, statement, context=None):
        raise NotImplementedError

    def read(self, statement, context=None):
        raise NotImplementedError

    def update(self, statement, context=None):
        raise NotImplementedError

    def delete(self, statement, context=None):
        raise NotImplementedError
