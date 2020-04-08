from bson.codec_options import TypeRegistry, CodecOptions

from pipeline.datasource import *
from pipeline.tasks import *
from pipeline.custom_codecs import *


def main():
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
    extraction = Extract(datasources=[postgres])
    getting_schema = GetSchema(datasources=[postgres])
    transforming = Transform(requires={'Extract': extraction, 'GetSchema': getting_schema}, dwh=mongo)
    loading = Load(requires={'Transform': transforming, "GetSchema": getting_schema}, dwh=mongo)
    clock = 0
    loading.launch()


if __name__ == '__main__':
    main()
