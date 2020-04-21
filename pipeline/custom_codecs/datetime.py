from bson.codec_options import TypeCodec
import datetime


class DatetimeCodec(TypeCodec):
    python_type = datetime.date
    bson_type = datetime.datetime

    def transform_bson(self, value):
        return value.date()

    def transform_python(self, value):
        return datetime.datetime.combine(value, datetime.datetime.min.time())
