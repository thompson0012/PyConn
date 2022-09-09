from addict import Addict
import pendulum


class MetaDataModel:
    def __init__(self):
        self._meta_data = Addict()
        self._meta_data._internal_log = []

    def add_meta_data(self, k, v):
        self._meta_data.update({k: v})

    def get_meta_data(self):
        return self._meta_data.to_dict()

    def add_action_time(self):
        self.add_meta_data('last_edited_at', pendulum.now('utc').isoformat())

    def to_dict(self):
        return self.__dict__
