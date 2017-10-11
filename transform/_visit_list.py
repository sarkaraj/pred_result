from pyspark.sql.types import *
from pyspark.sql.functions import *
import numpy as np
import datetime as dt
from properties import VISIT_LIST_LOCATION


def custom_parser(input):
    from datetime import datetime
    # input = input.replace('"', '')

    datetime_object = datetime.strptime(input, '\"%m/%d/%Y %I:%M:%S %p\"').strftime('%Y-%m-%d')

    return datetime_object


my_parser = udf(custom_parser, StringType())


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


def _schema_vl_2():
    # cdVisitInstance,cdClassification,Login,cdOrgUnit,nmDisplay,cdAccount,dsName,dtPlannedStart,dtPlannedEnd,dtLastModified,
    cdVisitInstance = StructField("cdVisitInstance", StringType(), nullable=True)
    cdClassification = StructField("cdClassification", StringType(), nullable=True)
    Login = StructField("Login", StringType(), nullable=True)
    cdOrgUnit = StructField("cdOrgUnit", StringType(), nullable=True)
    nmDisplay = StructField("nmDisplay", StringType(), nullable=True)
    cdAccount = StructField("cdAccount", StringType(), nullable=True)
    dsName = StructField("dsName", StringType(), nullable=True)
    dtPlannedStart = StructField("dtPlannedStart", StringType(), nullable=True)
    dtPlannedEnd = StructField("dtPlannedEnd", StringType(), nullable=True)
    dtLastModified = StructField("dtLastModified", StringType(), nullable=True)

    schema = StructType(
        [cdVisitInstance, cdClassification, Login, cdOrgUnit, nmDisplay, cdAccount, dsName, dtPlannedStart,
         dtPlannedEnd, dtLastModified])
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


def _get_visit_list_ver_2(sc, sqlContext, order_date):
    raw_data = sc.textFile(VISIT_LIST_LOCATION)
    header_vl = raw_data.first()

    vl = raw_data \
        .filter(lambda x: x != header_vl) \
        .map(lambda line: line.split(",")[:len(line.split(",")) - 1])

    # print vl.take(1)

    today = dt.date.today()

    vl_df = \
        sqlContext.createDataFrame(vl, schema=_schema_vl_2()) \
            .select('cdAccount', 'dtPlannedStart') \
            .withColumn('customernumber',
                        udf(lambda input: '0' + input.replace('"', ''), StringType())(col('cdAccount'))) \
            .withColumn('order_date', my_parser(col('dtPlannedStart'))) \
            .drop(col('cdAccount')) \
            .drop(col('dtPlannedStart'))

    return vl_df


if __name__ == "__main__":
    import numpy as np

    print np.busday_offset('2017-09-15', 2, roll='forward')
    print str(np.busday_offset('2017-09-15', 2, roll='forward'))
