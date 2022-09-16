import pyarrow as pa
import pyarrow.parquet as pq


class DBExportController:
    def __init__(self):
        pass


class BaseExporter:
    def __init__(self):
        pass


class ParquetExporter(BaseExporter):
    def __init__(self):
        super(ParquetExporter, self).__init__()


class CsvExporter(BaseExporter):
    def __init__(self):
        super(CsvExporter, self).__init__()


class JsonExporter(BaseExporter):
    def __init__(self):
        super(JsonExporter, self).__init__()
