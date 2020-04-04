from abc import *
import psycopg2
import pymongo
import pandas as pd
from pymongo.errors import CollectionInvalid


class DataSource(ABC):
    def __init__(self, name: str):
        # toDo change DataSource to uniquely indetify specific datasource
        self.name = name

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def pk_handler(self):
        pass

    @abstractmethod
    def fk_handler(self):
        pass

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def load(self, data: dict):
        pass

    @abstractmethod
    def drop_data(self):
        pass


class PSQLSource(DataSource):
    def __init__(self, user: str, password: str, dbname: str, host: str, port: int):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        super(PSQLSource, self).__init__(dbname)

    @staticmethod
    def get_table_names(cursor):
        tables_query = "select table_name, table_schema from information_schema.tables " \
                       "where table_schema not in ('pg_catalog', 'information_schema')"
        cursor.execute(tables_query)
        tables = cursor.fetchall()
        return tables

    def connect(self):
        try:
            pgconn = psycopg2.connect(user=self.user, password=self.password, host=self.host, port=self.port,
                                      database=self.name)
            return pgconn
        except (Exception, psycopg2.Error) as error:
            print('Failed to connect to PostgresSql', error)
            return None

    def pk_handler(self):
        conn = self.connect()
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
        conn = self.connect()
        cursor = conn.cursor()
        tables = PSQLSource.get_table_names(cursor)
        fk_map = {}
        for table in tables:
            table_name = str(table[0])
            table_schema = str(table[1])
            fk_query = "select kcu.table_schema as foreign_schema, kcu.table_name as foreign_table, kcu.column_name as fk_column" \
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
            for tpl in tpls:
                fk_map[table_name] = {'fschema': tpl[0], 'ftable': tpl[1], 'fcolumn': tpl[2]}
        cursor.close()
        return fk_map

    def extract(self):
        try:
            conn = self.connect()
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

    def load(self, data: dict):
        # toDo
        pass

    def drop_data(self):
        # toDO
        pass


class MongoSource(DataSource):
    def __init__(self, host: str, port: int, dbname: str, codec_options=None):
        self.host = host
        self.port = port
        self.codec_options = codec_options
        super(MongoSource, self).__init__(dbname)

    def connect(self):
        try:
            client = pymongo.MongoClient(host=self.host, port=self.port)
            mdbconn = client[self.name]
            return mdbconn
        except Exception as error:
            print('Failed to connect to MongoDb', error)

    def pk_handler(self):
        # toDo
        pass

    def fk_handler(self):
        # toDo
        pass

    def extract(self):
        try:
            conn = self.connect()
            collections = conn.list_collection_names()
            all_data = dict()
            for collection in collections:
                data = conn[collection].find()
                data_frame = pd.DataFrame(data)
                all_data[collection] = data_frame
            return all_data
        except Exception as error:
            print('Failed to extract data from mongo database', error)
            return None

    def load(self, data: dict):
        for data in data.values():
            if not isinstance(data, pd.DataFrame):
                raise KeyError
        conn = self.connect()
        for dataset_name in data.keys():
            # toDo check merely is such a collection exist or not
            try:
                collection = conn.create_collection(dataset_name, codec_options=self.codec_options, capped=False)
            except CollectionInvalid:
                collection = conn[dataset_name]
            df = data[dataset_name]
            collection_data = df.T.to_dict().values()
            collection.insert_many(collection_data)

    def drop_data(self):
        conn = self.connect()
        conn.drop_database(self.name)


class ExcelSource(DataSource):
    def __init__(self, filename):
        super(ExcelSource, self).__init__(filename)

    def extract(self):
        all_data = pd.read_excel(self.name)
        return all_data

    def pk_handler(self):
        # toDo
        pass

    def fk_handler(self):
        # toDo
        pass

    def load(self, data: dict):
        # toDo
        pass

    def drop_data(self):
        # toDo
        pass


class JSONSource(DataSource):
    def __init__(self, filename):
        super(JSONSource, self).__init__(filename)

    def extract(self):
        all_data = pd.read_json(self.name)
        return all_data

    def pk_handler(self):
        # toDo
        pass

    def fk_handler(self):
        # toDo
        pass

    def load(self, data: dict):
        # toDo
        pass
