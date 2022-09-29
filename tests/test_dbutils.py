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

        joiner = SqlJoiner()
        str_joined_dummy_data = joiner.join(parsed_dummy_data, 'stringify')

        json_joined_dummy_data = joiner.join(parsed_dummy_data, 'jsonify')

    @pysnooper.snoop()
    def test_func_substitute_sql(self):
        adapter = SqlTypeAdapter.from_default_mapper()
        adapter.register_mapper(datetime, lambda x: str(x))
        parsed_dummy_data = adapter.parse(self.dummy_data)

        joined_dummy_data = SqlJoiner().join(parsed_dummy_data, 'stringify')
        joined_dummy_data_json = SqlJoiner().join(parsed_dummy_data, 'jsonify')

        inject_template = 'insert into test_sync (id, truefalse) values {{values}}'

        inject_values = joined_dummy_data
        inject_values_json = joined_dummy_data_json

        substituted_sql = substitute_sql(inject_template, inject_values)






if __name__ == '__main__':
    unittest.main()
