import psycopg2
import pandas as pd

from pipeline.datasources.dataSource import DataSource


class PSQLSource(DataSource):
    def __init__(self, name: str, conn):
        self.conn = conn
        super(PSQLSource, self).__init__(name)

    @staticmethod
    def get_table_names(cursor):
        tables_query = "select table_name, table_schema from information_schema.tables " \
                       "where table_schema not in ('pg_catalog', 'information_schema')"
        cursor.execute(tables_query)
        tables = cursor.fetchall()
        return tables

    def pk_handler(self):
        conn = self.conn
        cursor = conn.cursor()
        tables = PSQLSource.get_table_names(cursor)
        pk_map = {}
        for table in tables:
            table_name = str(table[0])
            pk_query = "select a.attname, format_type(a.atttypid, a.atttypmod) AS data_type " \
                       "from   pg_index i " \
                       "join   pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)" \
                       f"where  i.indrelid = '{table_name}'::regclass and i.indisprimary;"
            cursor.execute(pk_query)
            pk_map[table_name] = list(cursor.fetchall())
        cursor.close()
        return pk_map

    def fk_handler(self):
        conn = self.conn
        cursor = conn.cursor()
        tables = PSQLSource.get_table_names(cursor)
        fk_map = {}
        for table in tables:
            table_name = str(table[0])
            table_schema = str(table[1])
            fk_query = "select rel_kcu.table_name as primary_table, kcu.column_name as fk_column" \
                       " from information_schema.table_constraints tco join information_schema.key_column_usage kcu " \
                       "on tco.constraint_schema = kcu.constraint_schema and tco.constraint_name = kcu.constraint_name " \
                       "join information_schema.referential_constraints rco on tco.constraint_schema = rco.constraint_schema " \
                       "and tco.constraint_name = rco.constraint_name join information_schema.key_column_usage rel_kcu " \
                       "on rco.unique_constraint_schema = rel_kcu.constraint_schema and rco.unique_constraint_name = rel_kcu.constraint_name " \
                       "and kcu.ordinal_position = rel_kcu.ordinal_position" \
                       f" where kcu.table_name = '{table_name}' and kcu.table_schema = '{table_schema}' " \
                       "and tco.constraint_type = 'FOREIGN KEY' order by kcu.table_schema, kcu.table_name, kcu.ordinal_position;"
            cursor.execute(fk_query)
            tpls = cursor.fetchall()  # tuples
            fk_map[table_name] = []
            for tpl in tpls:
                fk_map[table_name].append({'ftable': tpl[0], 'fcolumn': tpl[1]})
        cursor.close()
        return fk_map

    def extract(self):
        try:
            conn = self.conn
            cursor = conn.cursor()
            tables = self.get_table_names(cursor)
            all_data = dict()
            for table in tables:
                table_name = str(table[0])
                cursor.execute(f'select * from {table_name}')
                columns = [col[0] for col in cursor.description]
                data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                data_frame = pd.DataFrame(data)
                all_data[table_name] = data_frame
            cursor.close()
            return all_data
        except (Exception, psycopg2.Error) as error:
            print('Failed to extract data from psql database', error)
            return None
