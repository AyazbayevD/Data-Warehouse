from bson.codec_options import TypeCodec
import datetime
import decimal
from bson.decimal128 import Decimal128
from bson.binary import Binary
import pandas as pd


# toDo create another codecs

class DecimalCodec(TypeCodec):
    python_type = decimal.Decimal
    bson_type = Decimal128

    def transform_python(self, value):
        return Decimal128(value)

    def transform_bson(self, value):
        return value.to_decimal


class MemoryviewCodec(TypeCodec):
    python_type = memoryview
    bson_type = Binary

    def transform_python(self, value):
        return Binary(value)

    def transform_bson(self, value):
        return memoryview(value)


class DatetimeCodec(TypeCodec):
    python_type = datetime.date
    bson_type = datetime.datetime

    def transform_bson(self, value):
        return value.date()

    def transform_python(self, value):
        return datetime.datetime.combine(value, datetime.datetime.min.time())
