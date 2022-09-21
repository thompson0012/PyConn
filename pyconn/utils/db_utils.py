import re
from typing import List, Tuple
import json
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


class SqlResolver:
    def __init__(self, encoder=None):
        self._encoder = encoder

    def serialize(self, obj):
        return json.dumps(obj, cls=self._encoder)

    def rewrite(self, json_str) -> str:
        BRACKET_CONTENT_EXP = humre.OPEN_BRACKET + humre.ANYTHING + humre.CLOSE_BRACKET
        compiler = humre.compile(BRACKET_CONTENT_EXP)
        data_matched = compiler.findall(json_str[1:-1])

        def replace_bracket(obj):
            return '(' + obj[1:-1] + ')'

        return ','.join(list(map(lambda x: replace_bracket(x), data_matched)))
