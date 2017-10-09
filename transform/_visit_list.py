# from pyspark.sql.types import *
# from pyspark.sql.functions import *
import numpy as np
import datetime as dt
from properties import VISIT_LIST_LOCATION


def _schema_vl():
    MANDT = StructField("MANDT", StringType(), nullable=True)
    VPTYP = StructField("VPTYP", StringType(), nullable=True)
    EXDAT = StructField("EXDAT", StringType(), nullable=True)
    KUNNR = StructField("KUNNR", StringType(), nullable=True)
    FROMTIME = StructField("FROMTIME", StringType(), nullable=True)
    TOTIME = StructField("TOTIME", StringType(), nullable=True)
    USERID = StructField("USERID", StringType(), nullable=True)
    VKORG = StructField("VKORG", StringType(), nullable=True)
    VTWEG = StructField("VTWEG", StringType(), nullable=True)
    SPARTE = StructField("SPARTE", StringType(), nullable=True)
    AUTH = StructField("AUTH", StringType(), nullable=True)
    VLID = StructField("VLID", StringType(), nullable=True)
    VLPOS = StructField("VLPOS", StringType(), nullable=True)
    SEQU = StructField("SEQU", StringType(), nullable=True)
    VPID = StructField("VPID", StringType(), nullable=True)
    DATE_SENT = StructField("DATE_SENT", StringType(), nullable=True)
    TIME_SENT = StructField("TIME_SENT", StringType(), nullable=True)
    DELETE_FLAG = StructField("DELETE_FLAG", StringType(), nullable=True)
    VCTEXT = StructField("VCTEXT", StringType(), nullable=True)
    STIME = StructField("STIME", StringType(), nullable=True)
    STIMEZONE = StructField("STIMEZONE", StringType(), nullable=True)

    schema = StructType(
        [MANDT, VPTYP, EXDAT, KUNNR, FROMTIME, TOTIME, USERID, VKORG, VTWEG, SPARTE, AUTH, VLID, VLPOS, SEQU, VPID,
         DATE_SENT, TIME_SENT, DELETE_FLAG, VCTEXT, STIME, STIMEZONE])
    return schema


def _get_visit_list(sc, sqlContext, order_date):
    raw_data = sc.textFile(VISIT_LIST_LOCATION)
    header_vl = raw_data.first()

    vl = raw_data \
        .filter(lambda x: x != header_vl) \
        .map(lambda line: line.split(","))

    today = dt.date.today()

    vl_df = \
        sqlContext.createDataFrame(vl, schema=_schema_vl()) \
            .select(col('KUNNR'), col('EXDAT')) \
            .withColumn('order_date', from_unixtime(unix_timestamp(col('DATE_SENT'), "yyyy.MM.dd")).cast(DateType())) \
            .filter(col('order_date') == order_date)

    return vl_df


if __name__ == "__main__":
    import numpy as np

    print np.busday_offset('2017-09-15', 2, roll='forward')
    print str(np.busday_offset('2017-09-15', 2, roll='forward'))
