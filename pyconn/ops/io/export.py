import pyarrow as pa
import pyarrow.parquet as pq
import orjson
from typing import List, Tuple, Dict, Optional
from pyconn.utils.db_utils import SqlJoiner, SqlTypeAdapter, SqlSchemaOnWrite
import csv


class DBExportController:
    def __init__(self, format_):
        self._format = format_

    def export(self, filename, obj, col):
        match self._format:
            case 'csv':
                return CsvExporter().write_to_file(filename, obj, col)

            case 'parquet':
                return ParquetExporter().write_to_file(filename, obj, col)

            case 'json':
                return JsonExporter().write_to_file(filename, obj, col)

            case _:
                raise ValueError('not supported')


class BaseExporter:
    def __init__(self):
        self._adapter: Optional[SqlTypeAdapter] = None

    def register_type_adapter(self, adapter: SqlTypeAdapter):
        self._adapter: SqlTypeAdapter = adapter

    def register_mapper(self, type_, handler_func):
        if not self._adapter:
            self._adapter = SqlTypeAdapter.from_default_mapper()
        return self._adapter.register_mapper(type_, handler_func)

    def write_to_file(self, filename, obj, col):
        raise NotImplementedError


class ParquetExporter(BaseExporter):
    def __init__(self):
        super(ParquetExporter, self).__init__()

    def write_to_file(self, filename, obj, col):
        list(zip(*obj))


class CsvExporter(BaseExporter):
    def __init__(self):
        super(CsvExporter, self).__init__()

    def write_to_file(self, filename, obj: List[Tuple], col):
        with open(filename, 'w+') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(col)
            csv_writer.writerows(self.serialize(obj))

    def serialize(self, obj, columns=None):
        if not self._adapter:
            self._adapter = SqlTypeAdapter.from_default_mapper()
            self._adapter.register_mapper(type(None), lambda x: None)

        return self._adapter.parse(obj)


class JsonExporter(BaseExporter):
    def __init__(self):
        super(JsonExporter, self).__init__()

    def write_to_file(self, filename, obj, col=None):
        with open(filename, 'w') as file:
            file.write(self.serialize(obj, col))

    @classmethod
    def serialize(cls, obj, columns):
        data_map2col = list(map(lambda x: dict(zip(columns, x)), obj))
        return orjson.dumps(data_map2col).decode('utf-8')
