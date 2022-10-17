import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import orjson
from typing import List, Tuple, Dict, Optional
from pyconn.utils.db_utils import SqlJoiner, SqlTypeAdapter, SqlSchemaOnWrite
import csv
import io


class DBExportController:
    def __init__(self, format_):
        self._format = format_

    def export_to_file(self, filename, obj, col):
        match self._format:
            case 'csv':
                return CsvExporter().write_to_file(filename, obj, col)

            case 'parquet':
                return ParquetExporter().write_to_file(filename, obj, col)

            case 'json':
                return JsonExporter().write_to_file(filename, obj, col)

            case _:
                raise ValueError('not supported')

    def export_to_memory(self, obj, col):
        match self._format:
            case 'csv':
                return CsvExporter().writer_to_memory(obj, col)
            case 'parquet':
                return ParquetExporter().writer_to_memory(obj, col)
            case 'json':
                return JsonExporter().writer_to_memory(obj, col)
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

    def serialize(self, obj):
        if not self._adapter:
            self._adapter = SqlTypeAdapter.from_default_mapper()
            self._adapter.register_mapper(type(None), lambda x: None)

        return self._adapter.parse(obj)

    def write_to_file(self, filename, obj, col):
        raise NotImplementedError

    def writer_to_memory(self, obj, col):
        raise NotImplementedError


class ParquetExporter(BaseExporter):
    def __init__(self):
        super(ParquetExporter, self).__init__()

    def write_to_file(self, filename, obj, col):
        col_data = list(zip(*obj))
        parquet_table = pa.table(dict(zip(col, col_data)))
        pq.write_table(parquet_table, filename)
        return

    def writer_to_memory(self, obj, col):
        sink = pa.BufferOutputStream()

        col_data = list(zip(*obj))
        parquet_table = pa.table(dict(zip(col, col_data)))
        pq.write_table(parquet_table, sink)
        return sink.getvalue()


class CsvExporter(BaseExporter):
    def __init__(self):
        super(CsvExporter, self).__init__()

    def write_to_file(self, filename, obj: List[Tuple], col):
        with open(filename, 'w+') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(col)
            csv_writer.writerows(self.serialize(obj))

    def writer_to_memory(self, obj, col):
        s = io.StringIO()
        writer = csv.writer(s)
        writer.writerow(col)
        writer.writerows(obj)
        s.seek(0)

        buf = io.BytesIO()
        buf.write(s.getvalue().encode())
        buf.seek(0)
        return buf


class JsonExporter(BaseExporter):
    def __init__(self):
        super(JsonExporter, self).__init__()

    def write_to_file(self, filename, obj, col=None):
        with open(filename, 'w') as file:
            file.write(self.serialize(obj, col))

    def writer_to_memory(self, obj, col):
        return self.serialize(obj, col)

    @classmethod
    def serialize(cls, obj, columns):
        data_map2col = list(map(lambda x: dict(zip(columns, x)), obj))
        return orjson.dumps(data_map2col).decode('utf-8')
