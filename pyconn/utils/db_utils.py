import re
from typing import List, Tuple, Optional, Dict
import json
import orjson
import datetime
import humre
from pyconn.utils.validator import validate_opts_value, validate_opts_type


def tuple_to_dict(tuple_values, dict_key):
    return dict(zip(dict_key, tuple_values))


def substitute_sql(template: str, values: str, placeholder='{values}'):
    import re
    # return re.sub("(?<![\w\d]){placeholder}(?![\w\d])".format(placeholder=placeholder),
    #               str(values)[1:-1],
    #               template)
    return re.sub("(?<![\w\d]){placeholder}(?![\w\d])".format(placeholder=placeholder), values, template)


def remove_all_line_breaks(string: str):
    compiler = re.compile('/(\r\n)+|\r+|\n+|\t+/')
    return compiler.sub("", string)


class ExtJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


class SqlBatchJoiner:
    def __init__(self):
        pass

    def serialize_join(self, obj: list):
        def serialized(obj: tuple):
            string_ = orjson.dumps(obj, option=orjson.OPT_UTC_Z).decode('utf-8')
            return '(' + string_[1:-1] + ')'

        sql_container = [serialized(item) for item in obj]

        return ','.join(sql_container)


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

    def parse(self, rows: List[Tuple]):
        raise NotImplementedError


class SqlTypeConverter(BaseSqlTypeConvAdap):
    DEFAULT_DTYPE = (int, str, float, bool)

    def __init__(self, mapper: Optional[Dict] = None):
        super(SqlTypeConverter, self).__init__(mapper)

    def init_default_mapper(self):
        for i in self.DEFAULT_DTYPE:
            self.register_mapper(i, i)

        self.register_mapper(None, lambda x: 'null')
        return

    def parse(self, rows: List[Tuple]):
        pass


class SqlTypeAdapter(BaseSqlTypeConvAdap):
    DEFAULT_DTYPE = (int, str, float, bool, bytes, complex)

    def __init__(self, mapper=None):
        super(SqlTypeAdapter, self).__init__(mapper)

    def init_default_mapper(self):
        for i in self.DEFAULT_DTYPE:
            self.register_mapper(i, i)

        self.register_mapper(None, lambda x: 'null')
        return

    def parse(self, rows: List[Tuple]):
        validate_opts_type(rows, list)
        validate_opts_type(rows[0], tuple)

        # rows_record = []
        # for row in rows:
        #     row_record = []
        #     for i, col in enumerate(row):
        #         if isinstance(col, self.DEFAULT_DTYPE):
        #             row_record.append(col)
        #             continue
        #         row_record.append(self._mapper[type(col)](col))
        #
        #     rows_record.append(tuple(row_record))

        def col_adapt(col):
            if isinstance(col, self.DEFAULT_DTYPE):
                return col
            return self._mapper[type(col)](col)

        def row_adapt(row):
            return tuple(map(col_adapt, row))

        return tuple(map(row_adapt, rows))

