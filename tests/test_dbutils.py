import pprint
import unittest
from pyconn.utils.db_utils import *
from datetime import datetime
import pysnooper


class DbUtilsTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.dummy_data = [(1, 2, 3, None), (datetime.utcnow(), 1, 3, 5, 7, 9), ('{"json":{"json2":123}}',)]

    @pysnooper.snoop()
    def test_SqlTypeAdapter(self):
        adapter = SqlTypeAdapter.from_default_mapper()
        adapter.register_mapper(datetime, lambda x: str(x))
        parsed_dummy_data = adapter.parse(self.dummy_data)

    @pysnooper.snoop()
    def test_SqlBatchJoiner_join(self):
        adapter = SqlTypeAdapter.from_default_mapper()
        adapter.register_mapper(datetime, lambda x: str(x))
        parsed_dummy_data = adapter.parse(self.dummy_data)

        joiner = SqlBatchJoiner()
        str_joined_dummy_data = joiner.join(parsed_dummy_data, 'stringify')

        json_joined_dummy_data = joiner.join(parsed_dummy_data, 'jsonify')

    @pysnooper.snoop()
    def test_func_substitute_sql(self):
        adapter = SqlTypeAdapter.from_default_mapper()
        adapter.register_mapper(datetime, lambda x: str(x))
        adapter.register_mapper(type(None), lambda x: '`null`')
        parsed_dummy_data = adapter.parse(self.dummy_data)
        joined_dummy_data = SqlBatchJoiner().join(parsed_dummy_data, 'stringify')
        inject_template = 'insert into test_sync (id, truefalse) values {{values}}'
        inject_values = joined_dummy_data

        def substitute_sql(template: str, values: str, placeholder='{{values}}'):
            import re
            # return re.sub("(?<![\w\d]){placeholder}(?![\w\d])".format(placeholder=placeholder),
            #               str(values)[1:-1],
            #               template)
            sub_placeholder_sql = re.sub("(?<![\w\d]){{values}}(?![\w\d])", values,
                                         template)
            return re.sub('`null`', 'null', sub_placeholder_sql)

        substituted_sql = substitute_sql(inject_template, inject_values)
        print(substituted_sql)


if __name__ == '__main__':
    unittest.main()
