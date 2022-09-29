import re
from typing import List, Tuple, Optional, Dict
import json
import orjson
import datetime
import humre
from types import NoneType
from pyconn.utils.validator import validate_opts_value, validate_opts_type


def tuple_to_dict(tuple_values, dict_key):
    return dict(zip(dict_key, tuple_values))


# I think there have another smart way to substitute the sql
# SQL injection should only be occurred when user operates in something that is not author's original intend
def substitute_sql(template: str, values: str, placeholder='{{values}}', null_handle=True):
    sub_sql = re.sub("(?<![\w\d]){placeholder}(?![\w\d])".format(placeholder=placeholder), values, template)
    if null_handle:
        return re.sub("'null'", "null", sub_sql)
    return sub_sql


def remove_all_line_breaks(string: str):
    compiler = re.compile('/(\r\n)+|\r+|\n+|\t+/')
    return compiler.sub("", string)


class ExtJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


class SqlJoiner:
    def __init__(self):
        pass

    def stringify_join(self, obj: tuple | list):
        from functools import reduce

        def row_adapt(row: tuple):
            if len(row) == 1:
                return re.sub(',', '', str(row))
            else:
                return str(row)

        return reduce(lambda a, b: ','.join([a, b]), map(row_adapt, obj))

    def jsonify_join(self, obj: tuple | list):
        def serialized(obj):
            string_ = orjson.dumps(obj, option=orjson.OPT_UTC_Z).decode('utf-8')
            return '(' + string_[1:-1] + ')'

        sql_container = [serialized(item) for item in obj]

        return ','.join(sql_container)

    def join(self, obj, method: str):
        match method:
            case 'stringify':
                return self.stringify_join(obj)

            case 'jsonify':
                return self.jsonify_join(obj)

            case _:
                raise ValueError('not supported')


class SqlSchemaOnWrite:
    def __init__(self):
        pass

    @classmethod
    def parse(cls, rows: List[Tuple]):
        validate_opts_type(rows, list)
        validate_opts_type(rows[0], tuple)
        schema = {}
        for i, val in enumerate(rows[0]):
            schema[i] = type(val)

        return schema


class BaseSqlTypeConvAdap:
    def __init__(self, mapper):
        self._mapper = mapper

    def get_mapper(self):
        return self._mapper

    def register_mapper(self, val_type, handle_func):
        if not self._mapper:
            self._mapper = {}

        self._mapper[val_type] = handle_func
        return

    def init_default_mapper(self):
        raise NotImplementedError

    @classmethod
    def from_default_mapper(cls):
        adapt = cls()
        adapt.init_default_mapper()
        return adapt

    def parse(self, rows: List[Tuple]):
        raise NotImplementedError


class SqlTypeAdapter(BaseSqlTypeConvAdap):
    DEFAULT_DTYPE = (int, str, float, bool, bytes, complex)

    def __init__(self, mapper=None):
        super(SqlTypeAdapter, self).__init__(mapper)

    def init_default_mapper(self):
        for i in self.DEFAULT_DTYPE:
            self.register_mapper(i, i)

        self.register_mapper(NoneType, lambda x: 'null')
        return

    def parse(self, rows: List[Tuple]):
        validate_opts_type(rows, (tuple, list))
        validate_opts_type(rows[0], tuple)

        def col_adapt(col):
            if isinstance(col, self.DEFAULT_DTYPE):
                return col
            return self._mapper[type(col)](col)

        def row_adapt(row):
            return tuple(map(col_adapt, row))

        return tuple(map(row_adapt, rows))
