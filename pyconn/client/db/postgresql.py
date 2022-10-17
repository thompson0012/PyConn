from pyconn.client.db.base import BaseDBClient
import asyncio
import asyncpg
import psycopg
from typing import List
import humre
from pyconn.utils.validator import validate_opts_value
from pyconn.utils.db_utils import tuple_to_dict


class PostgresSQLClient(BaseDBClient):
    def __init__(self, db_params):
        super(PostgresSQLClient, self).__init__(db_params)

    def connect(self):
        conn = psycopg.connect(**self.get_db_params())
        self._conn = conn
        self._cursor = conn.cursor()
        return self

    def show_table_schema(self, tbl_name):
        data = self.execute('select * from information_schema.columns where table_schema=%s and table_name=%s',
                            ('public', tbl_name)).fetchall()
        return map(lambda x: tuple_to_dict(x, ['table_catalog', 'table_schema', 'table_name', 'column_name',
                                               'ordinal_position', 'column_default', 'is_nullable', 'data_type',
                                               'character_maximum_length', 'character_octet_length',
                                               'numeric_precision', 'numeric_precision_radix',
                                               'numeric_scale', 'datetime_precision', 'interval_type',
                                               'interval_precision', 'character_set_catalog', 'character_set_schema',
                                               'character_set_name', 'collation_catalog', 'collation_schema',
                                               'collation_name', 'domain_catalog', 'domain_schema', 'domain_name',
                                               'udt_catalog', 'udt_schema', 'udt_name', 'scope_catalog', 'scope_schema',
                                               'scope_name', 'maximumcardinality', 'dtd_identifier',
                                               'is_self_referencing', 'is_identity', 'identity_generation',
                                               'identity_start', 'identity_increment', 'identity_maximum',
                                               'identity_minimum', 'identity_cycle', 'is_generated',
                                               'generation_expression', 'is_updatable']), data)

    def show_table_ddl(self, tbl_name):
        self.execute("""CREATE OR REPLACE FUNCTION generate_create_table_statement(p_table_name varchar)
  RETURNS text AS
$BODY$
DECLARE
    v_table_ddl   text;
    column_record record;
BEGIN
    FOR column_record IN 
        SELECT 
            b.nspname as schema_name,
            b.relname as table_name,
            a.attname as column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) as column_type,
            CASE WHEN 
                (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                 FROM pg_catalog.pg_attrdef d
                 WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) IS NOT NULL THEN
                'DEFAULT '|| (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                              FROM pg_catalog.pg_attrdef d
                              WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef)
            ELSE
                ''
            END as column_default_value,
            CASE WHEN a.attnotnull = true THEN 
                'NOT NULL'
            ELSE
                'NULL'
            END as column_not_null,
            a.attnum as attnum,
            e.max_attnum as max_attnum
        FROM 
            pg_catalog.pg_attribute a
            INNER JOIN 
             (SELECT c.oid,
                n.nspname,
                c.relname
              FROM pg_catalog.pg_class c
                   LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              WHERE c.relname ~ ('^('||p_table_name||')$')
                AND pg_catalog.pg_table_is_visible(c.oid)
              ORDER BY 2, 3) b
            ON a.attrelid = b.oid
            INNER JOIN 
             (SELECT 
                  a.attrelid,
                  max(a.attnum) as max_attnum
              FROM pg_catalog.pg_attribute a
              WHERE a.attnum > 0 
                AND NOT a.attisdropped
              GROUP BY a.attrelid) e
            ON a.attrelid=e.attrelid
        WHERE a.attnum > 0 
          AND NOT a.attisdropped
        ORDER BY a.attnum
    LOOP
        IF column_record.attnum = 1 THEN
            v_table_ddl:='CREATE TABLE '||column_record.schema_name||'.'||column_record.table_name||' (';
        ELSE
            v_table_ddl:=v_table_ddl||',';
        END IF;

        IF column_record.attnum <= column_record.max_attnum THEN
            v_table_ddl:=v_table_ddl||chr(10)||
                     '    '||column_record.column_name||' '||column_record.column_type||' '||column_record.column_default_value||' '||column_record.column_not_null;
        END IF;
    END LOOP;

    v_table_ddl:=v_table_ddl||');';
    RETURN v_table_ddl;
END;
$BODY$
  LANGUAGE 'plpgsql' COST 100.0 SECURITY INVOKER;""")
        ddl = self.execute('select generate_create_table_statement(\'%s\')', tbl_name)
        return map(lambda x: tuple_to_dict(x, ['sql']), ddl)


class AsyncPostgresSQLClient(PostgresSQLClient):
    def __init__(self, db_params):
        super(AsyncPostgresSQLClient, self).__init__(db_params)
