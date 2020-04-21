from bson.codec_options import TypeRegistry, CodecOptions

from pipeline.custom_codecs.datetime import DatetimeCodec
from pipeline.custom_codecs.memoryView import MemoryviewCodec
from pipeline.datasources.csvSource import CSVSource
from pipeline.datasources.mongoSource import MongoSource
from pipeline.datasources.postgresSql import PSQLSource
from pipeline.parsers.tableLayout import TableLayoutParser
from pipeline.custom_codecs.decimal import *
from pipeline.tasks.extract import Extract
from pipeline.tasks.getSchema import GetSchema
from pipeline.tasks.load import Load
from pipeline.tasks.transform import Transform


def main():
    tl_parser = TableLayoutParser('https://pcms.university.innopolis.ru/results/innopolis/2018-2019/final/')
    tl_parser.parse()
    csv = CSVSource(
        'temp_files/Финальный_этап_Олимпиады_Университета_Иннополис_Innopolis_Open_Final_Round_2018_2019.csv')
    postgres = PSQLSource(
        user='postgres',
        password='',
        host='localhost',
        port=5432,
        dbname='dvdrental'
    )
    decimal_codec = DecimalCodec()
    memoryview_codec = MemoryviewCodec()
    datetime_codec = DatetimeCodec()
    type_registry = TypeRegistry([decimal_codec, memoryview_codec, datetime_codec])
    codec_options = CodecOptions(type_registry=type_registry)
    mongo = MongoSource(
        host='localhost',
        port=27017,
        dbname='dvdrental',
        codec_options=codec_options
    )
    extraction = Extract(datasources=[csv])
    getting_schema = GetSchema(datasources=[csv])
    transforming = Transform(requires={'Extract': extraction, 'GetSchema': getting_schema})
    loading = Load(requires={'Transform': transforming}, dwh=mongo)
    loading.launch()


if __name__ == '__main__':
    main()
