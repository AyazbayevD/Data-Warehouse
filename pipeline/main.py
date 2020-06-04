from bson.codec_options import TypeRegistry, CodecOptions
from pipeline.custom_codecs.datetime import DatetimeCodec
from pipeline.custom_codecs.memoryView import MemoryviewCodec
from pipeline.datasources.csvSource import CSVSource
from pipeline.datawarehouse.mongoDwh import MongoDwh
from pipeline.parsers.pcmsParser import TableLayoutParser
from pipeline.custom_codecs.decimal import *
from pipeline.tasks.extract import Extract
from pipeline.tasks.getSchema import GetSchema
from pipeline.tasks.load import Load
from pipeline.tasks.personTracker import PersonTracker
from pipeline.tasks.transform import Transform

import pymongo
import pandas as pd


def connect_mongo(host: str, port: int, dbname: str):
    client = pymongo.MongoClient(host, port)
    conn = client[dbname]
    return conn


def main():
    csv = CSVSource('sample',
                    'temp_files/file.csv')
    decimal_codec = DecimalCodec()
    memoryview_codec = MemoryviewCodec()
    datetime_codec = DatetimeCodec()
    type_registry = TypeRegistry([decimal_codec, memoryview_codec, datetime_codec])
    codec_options = CodecOptions(type_registry=type_registry)
    mongo = MongoDwh(
        conn=connect_mongo(host='localhost', port=27017, dbname='dvdrental'),
        custom_codecs=codec_options
    )
    extraction = Extract(datasources=[csv])
    getting_schema = GetSchema(datasources=[csv])
    transforming = Transform(requires={'Extract': extraction, 'GetSchema': getting_schema})
    loading = Load(requires={'Transform': transforming}, dwh=mongo)
    person_tracker = PersonTracker(requires={'Extract': extraction}, dwh=mongo)
    person_tracker.launch()
    loading.launch()


if __name__ == '__main__':
    main()
