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


class SqlBatchJoiner:
    def __init__(self):
        pass

    def serialize_join(self, obj: list):

        def serialized(obj: tuple):
            string_ = orjson.dumps(obj, option=orjson.OPT_UTC_Z).decode('utf-8')
            return '(' + string_[1:-1] + ')'
        sql_container = [serialized(item) for item in obj]

        return ','.join(sql_container)
