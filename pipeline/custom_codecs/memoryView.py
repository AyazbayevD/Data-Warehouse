from bson.binary import Binary
from bson.codec_options import TypeCodec


class MemoryviewCodec(TypeCodec):
    python_type = memoryview
    bson_type = Binary

    def transform_python(self, value):
        return Binary(value)

    def transform_bson(self, value):
        return memoryview(value)
