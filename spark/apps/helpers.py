import hashlib
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType


def hash_str_to_int(text):
    if text is None:
        text = "None"
    h = int(hashlib.sha256(text.encode('utf-8')).hexdigest(), 16)
    return int(h % 10**4)


hash_str_to_int_udf = udf.udf(hash_str_to_int, IntegerType())
