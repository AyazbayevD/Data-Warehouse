import pymongo
import pandas as pd
from pymongo.errors import CollectionInvalid
import bson

from pipeline.datasources.dataSource import DataSource


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

    def load(self, data: dict, schema: dict):
        conn = self.connect()
        for source_name in data.keys():
            source_data = data[source_name]
            for dataset_name in source_data.keys():
                df = source_data[dataset_name]
                if type(df) != pd.DataFrame:
                    raise ValueError
                try:
                    collection = conn.create_collection(dataset_name, codec_options=self.codec_options,
                                                        capped=False)
                except CollectionInvalid:
                    collection = conn[dataset_name]
                collection_data = df.T.to_dict().values()
                collection.insert_many(collection_data)
        #fixing ids
        for source_name in schema.keys():
            source_schema = schema[source_name]
            for dataset_name in data[source_name].keys():
                dependencies = source_schema['dependencies'][dataset_name]
                if not len(dependencies):
                    continue
                references = {}
                for dependence in dependencies:
                    if dependence['ftable'] in references:
                        references[dependence['ftable']].append(dependence['fcolumn'])
                    else:
                        references[dependence['ftable']] = [dependence['fcolumn']]
                collection = conn[dataset_name]
                docs = collection.find()
                for doc in docs:
                    for ftable_name in references.keys():
                        ftable = conn[ftable_name]
                        fdoc_info = dict(
                            zip(references[ftable_name], [doc[fcolumn] for fcolumn in references[ftable_name]])
                        )
                        fdoc = ftable.find_one(fdoc_info)
                        to_unset = dict(
                                        zip(references[ftable_name], [' ' for fcolumn in references[ftable_name]]))
                        if fdoc is None:
                            continue
                        collection.update_one(
                            {'_id': doc['_id']},
                            {
                                '$set': {
                                    f'{ftable_name}': bson.DBRef(ftable_name, fdoc['_id'])
                                },
                                '$unset': to_unset
                            },
                        )

    def drop_data(self):
        client = pymongo.MongoClient(host=self.host, port=self.port)
        client.drop_database(self.name)
