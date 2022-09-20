import re
from typing import List, Tuple


def tuple_to_dict(tuple_values, dict_key):
    return dict(zip(dict_key, tuple_values))


def substitute_sql(template: str, values: List[Tuple], placeholder='{values}'):
    import re
    return re.sub("(?<![\w\d]){placeholder}(?![\w\d])".format(placeholder=placeholder),
                  str(values)[1:-1],
                  template)


def remove_all_line_breaks(string: str):
    compiler = re.compile('/(\r\n)+|\r+|\n+|\t+/')
    return compiler.sub("", string)
