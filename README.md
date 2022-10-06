# what is pyconn?

PyConn is a connection tool designed for linking database, object storage system.
it aims to provide seamlessly connection experience through uniform style of APIs

# why need pyconn

PyConn is born with the wish to advance python developer's experience.
every analytics system are about to data storage, shipping data from A to B.
developer have to build their database connection tool over, over and over again.

just imagine you are data engineer, scientists, architects of system. there are lots of production/legacy system storing
your
enterprise data, now you are being asked to integrate all of them into a single source of system (data warehouse).

- if you are using spark for data processing, there are set of configuration have to done before any data shipping job,
  such as JDBC drivers
  for different databases


- if you are using kafka/pulsar for data streaming, you also have to write your connection script for message transfer


- if you are migrating the structured data to another storage system for OLAP, such as MySql -> BigQuery, you have to
  make sure the data
  schema is matching with the destination system, developer have to build their specific connection script.

is there any a uniform package that can connect much type database include but not limited to SQL/NoSql/DataLake ?
easily syncing data? also easily extend when new database is developed from community?

we need a tool that can fill the gap, that's what PyConn to do!

# install

```shell
pip install pyconn
```

# features

1. uniform connection experience

```python
from pyconn.api import MySQLClient

client = MySQLClient.from_kv(database='test', host='localhost', port=3306, user='admin', password='admin')
client.connect()

```

```python
from pyconn.api import SQLiteClient

client = SQLiteClient.from_kv(datbase='./test.db')
client.connect()
```

2. data lake support

```python
from pyconn.api import S3Client, GCSClient

client = GCSClient.from_credential_files('gcs_credentials.json')
client.connect()
```

```python
# aws resource have been in os.environment
from pyconn.api import S3Client

client = S3Client(lake_params={'bucket': 'test'})
client.connect()
```

3. multiple database sync support

- upsert
- full
- general

```python
from pyconn.api import UpsertDBSyncClient
from pyconn.api import MySQLClient, SQLiteClient

client = MySQLClient.from_kv(database='test', host='localhost', port=3306, user='admin', password='admin')
sc = SQLiteClient.from_kv(database='./test.db')

client = UpsertDBSyncClient()
client.register_source(sc)
client.register_target(mc)
client.register_load_sql('replace into test_sync (id, name, password) values {{values}}')
client.register_extract_sql('select id,name,password from test_sync')
client.sync(100)
```

4. database io, and type adapter

```python
from pyconn.api import ParquetExporter, JsonExporter, CsvExporter
from pyconn.api import MySQLClient
from datetime import datetime

client = MySQLClient.from_kv(database='test', host='localhost', port=3306, user='admin', password='admin')
client.connect()

q = client.execute('select * from test_sync', keep_alive=True)
rows = q.fetchall()

exporter = JsonExporter()
exporter.write_to_file('rows.json', rows, ['id', 'name', 'password'])

exporter = CsvExporter()
exporter.register_mapper(datetime, lambda x: str(x))
exporter.write_to_file('rows.csv', rows, ['id', 'name', 'password'])

exporter = ParquetExporter()
exporter.write_to_file('rows.parquet', rows, ['id', 'name', 'password'])
```


# development progress
for the release notes and development progress, pls find the [following files](release%20notes.md)

