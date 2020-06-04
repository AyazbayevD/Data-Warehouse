from pipeline.datawarehouse.dataWarehouse import DataWarehouse
import pymongo
import pandas as pd
from pymongo.errors import CollectionInvalid
import bson


class MongoDwh(DataWarehouse):
    def __init__(self, conn, custom_codecs):
        self.codec_options = custom_codecs
        super(MongoDwh, self).__init__(conn)

    def load(self, data: dict, schema: dict):
        for source_name in data.keys():
            source_data = data[source_name]
            for dataset_name in source_data.keys():
                df = source_data[dataset_name]
                if type(df) != pd.DataFrame:
                    raise ValueError
                try:
                    collection = self.conn.create_collection(dataset_name, codec_options=self.codec_options,
                                                             capped=False)
                except CollectionInvalid:
                    self.drop_table(dataset_name)
                    collection = self.conn.create_collection(dataset_name, codec_options=self.codec_options,
                                                             capped=False)
                collection_data = df.T.to_dict().values()
                collection.insert_many(collection_data)
        self.fixing_ids(schema)

    def fixing_ids(self, schema: dict):
        for source_name in schema.keys():
            source_schema = schema[source_name]
            for dataset_name in source_schema['dependencies'].keys():
                dependencies = source_schema['dependencies'][dataset_name]
                if not len(dependencies):
                    continue
                references = {}
                for dependence in dependencies:
                    if dependence['ftable'] in references:
                        references[dependence['ftable']].append(dependence['fcolumn'])
                    else:
                        references[dependence['ftable']] = [dependence['fcolumn']]
                collection = self.conn[dataset_name]
                docs = collection.find()
                for doc in docs:
                    for ftable_name in references.keys():
                        ftable = self.conn[ftable_name]
                        fdoc_info = dict(
                            zip(references[ftable_name], [doc[fcolumn] for fcolumn in references[ftable_name]])
                        )
                        fdoc = ftable.find_one(fdoc_info)
                        to_unset = dict(
                            zip(references[ftable_name], [' ' for _ in references[ftable_name]])
                        )
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

    def get_table(self, table_name: str):
        table = self.conn[table_name]
        data = table.find()
        return pd.DataFrame(data)

    def drop_table(self, table_name: str):
        table = self.conn[table_name]
        table.remove()

    def add_mention(self, info: dict):
        mentions = self.conn.get_collection('Mentions', codec_options=self.codec_options)
        mentions.insert_one(info)

    def add_person(self, info: dict):
        if info['last_name'] and info['first_name']:
            confidence = 1
        elif info['email'] or info['first_name'] or info['phone_number']:
            confidence = (bool(info['email']) + bool(info['first_name']) + bool(info['phone_number'])) / 3 * 0.5
        else:
            confidence = 0.25
        info['confidence'] = confidence
        people = self.conn.get_collection('People', codec_options=self.codec_options)
        person_id = people.insert(info)
        person = list(people.find({'_id': person_id}))
        return pd.DataFrame(person)

    def find_people(self, info: dict):
        ftr = {}
        for k, v in info.items():
            if v:
                ftr[k] = v
        people = self.conn.get_collection('People', codec_options=self.codec_options)
        related = list(people.find(ftr))
        if not len(related):
            return self.add_person(info)
        else:
            return pd.DataFrame(related)
