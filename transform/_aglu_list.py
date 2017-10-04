from pyspark.sql.types import *
from pyspark.sql.functions import *
# import numpy as np
# import datetime as dt
from properties import AGLU_LIST_LOCATION


def _schema_aglu():
    MANDT = StructField("MANDT", StringType(), nullable=True)
    VKORG = StructField("VKORG", StringType(), nullable=True)
    VTWEG = StructField("VTWEG", StringType(), nullable=True)
    SCL_AUTH_MATLST = StructField("SCL_AUTH_MATLST", StringType(), nullable=True)
    MATNR = StructField("MATNR", StringType(), nullable=True)
    LOEVM = StructField("LOEVM", StringType(), nullable=True)
    AENDT = StructField("AENDT", StringType(), nullable=True)
    AENUHR = StructField("AENUHR", StringType(), nullable=True)

    schema = StructType([MANDT, VKORG, VTWEG, SCL_AUTH_MATLST, MATNR, LOEVM, AENDT, AENUHR])

    return schema


def _get_aglu_list(sc, sqlContext):
    raw_data = sc.textFile(AGLU_LIST_LOCATION)
    header_aglu = raw_data.first()

    aglu_clean = raw_data \
        .filter(lambda x: x != header_aglu) \
        .map(lambda line: line.split(","))

    aglu_df = sqlContext.createDataFrame(aglu_clean, schema=_schema_aglu())

    return aglu_df
