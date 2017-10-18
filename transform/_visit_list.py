from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import datetime as dt
from properties import VISIT_LIST_LOCATION


def custom_parser(input):
    from datetime import datetime
    # input = input.replace('"', '')

    datetime_object = datetime.strptime(input, '\"%m/%d/%Y %I:%M:%S %p\"').strftime('%Y-%m-%d')

    return datetime_object


# my_parser = udf(custom_parser, StringType())


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


def _get_visit_list(sc, sqlContext, order_date, **kwargs):
    if 'visit_list' in kwargs.keys():
        vl = kwargs.get('visit_list')
    else:
        raw_data = sc.textFile(VISIT_LIST_LOCATION)
        header_vl = raw_data.first()

        vl_temp = raw_data \
            .filter(lambda x: x != header_vl) \
            .map(lambda line: line.split(","))

        vl = sqlContext.createDataFrame(vl_temp, schema=_schema_vl()) \
            .withColumn('order_date',
                        from_unixtime(unix_timestamp(col('DATE_SENT'), "yyyy.MM.dd")).cast(DateType()).alias(
                            'order_date')) \
            .select(col('KUNNR').alias('customernumber'), col('order_date'))

    vl_df = vl \
        .filter(col('order_date') == order_date)

    return vl_df


# # NOT USED TILL NOW -- MOST LIKELY TO BE DELETED
def _get_generated_visit_list(sc, sqlContext):
    invoice_q = """
    select d.customernumber customernumber, d.matnr matnr, d.bill_date bill_date, IF(d.units != 'CS', d.quantity * (f.umrez / f.umren), d.quantity) quantity, d.dlvry_lag dlvry_lag
    from
    (
    select b.customernumber customernumber, b.matnr matnr, b.bill_date bill_date ,b.quantity quantity, b.units units, b.price price, c.dlvry_lag dlvry_lag
    from
    (
    select a.kunag customernumber, a.matnr matnr, a.fkdat bill_date ,a.fklmg quantity, a.meins units, a.netwr price
    from skuopt.invoices a
    where a.kunag in ('0500083147','0500061438','0500067084','0500058324','0500080723','0500060033','0500068825','0500060917','0500078551','0500076115','0500071747','0500078478','0500078038','0500073982','0500064458','0500268924','0500070702','0500070336','0500076032','0500095883','0500284889')
    ) b
    join
    (
    select kunnr customernumber, IF(vsbed == '01', 2, 1) dlvry_lag
    from mdm.customer
    where vkbur='C005'
    ) c
    on
    b.customernumber = c.customernumber
    ) d
    join
    (
    select e.matnr matnr, e.meinh meinh, e.umren umren, e.umrez umrez
    from mdm.dim_marm e
    ) f
    on
    d.matnr=f.matnr and d.units=f.meinh
    where d.bill_date >= '20170903' and d.bill_date <= '20171007'
    """

    invoice_raw = sqlContext.sql(invoice_q)
    invoice_raw.cache()

    data_set = invoice_raw \
        .filter(col('quantity') > 0) \
        .withColumn('b_date', from_unixtime(unix_timestamp(col('bill_date'), "yyyyMMdd")).cast(DateType())) \
        .withColumn('dlvry_date', udf((lambda x: x.strftime('%Y-%m-%d')), StringType())(col('b_date'))) \
        .withColumn('bus_day_flag',
                    udf((lambda x: str(np.is_busday([x])[0])), StringType())(col('dlvry_date')).cast(BooleanType())) \
        .withColumn('visit_date',
                    udf((lambda x, y: str(np.busday_offset(x, -y, roll='backward'))), StringType())(col('dlvry_date'),
                                                                                                    col('dlvry_lag'))) \
        .select(col('customernumber'), col('visit_date')) \
        .distinct()
    return None

if __name__ == "__main__":
    import numpy as np

    print np.busday_offset('2017-09-15', 2, roll='forward')
    print str(np.busday_offset('2017-09-15', 2, roll='forward'))
