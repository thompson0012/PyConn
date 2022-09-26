import re
from typing import List, Tuple
import json
import orjson
import datetime
import humre


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


class SyncSqlResolver:
    def __init__(self, regex=None, encoder=None):
        self._regex = regex
        self._encoder = encoder
        self._regex_compiler = humre.compile(regex)

    @classmethod
    def from_default(cls):
        BRACKET_CONTENT_EXP = humre.OPEN_BRACKET + humre.ANYTHING + humre.CLOSE_BRACKET
        return cls(regex=BRACKET_CONTENT_EXP, encoder=ExtJsonEncoder)

    def serialize(self, obj) -> "json string":
        # return json.dumps(obj, cls=self._encoder, ensure_ascii=False)
        return orjson.dumps(obj, option=orjson.OPT_UTC_Z).decode('utf-8')

    def rewrite(self, json_str) -> str:
        data_matched = self._regex_compiler.findall(json_str[1:-1])

        def paren_first_last(obj):
            return '(' + obj[1:-1] + ')'

        return ','.join(list(map(lambda x: paren_first_last(x), data_matched)))


class SqlBatchJoiner:
    def __init__(self):
        pass

    def serialize_join(self, obj: list):

        def serialized(obj: tuple):
            string_ = orjson.dumps(obj, option=orjson.OPT_UTC_Z).decode('utf-8')
            return '(' + string_[1:-1] + ')'
        sql_container = [serialized(item) for item in obj]

        return ','.join(sql_container)
