from bson.codec_options import TypeCodec
import decimal
from bson.decimal128 import Decimal128


class DecimalCodec(TypeCodec):
    python_type = decimal.Decimal
    bson_type = Decimal128

    def transform_python(self, value):
        return Decimal128(value)

    def transform_bson(self, value):
        return value.to_decimal
