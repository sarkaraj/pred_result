from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from transform._visit_list import _get_visit_list
from transform._aglu_list import _get_aglu_list
from transform._invoice_latest import _get_invoice_data
from transform._prediction_list import _get_prediction_list
from support_func import get_current_date, get_order_dates_between, get_criteria_date
from properties import START_DATE_ORDER, END_DATE_ORDER, FINAL_PREDICTION_LOCATION

appName = "_".join(["CSO_TESTING_", get_current_date()])
conf = SparkConf() \
    .setAppName(appName) \
    .set("spark.io.compression.codec", "snappy")

sc = SparkContext(conf=conf)
sqlContext = HiveContext(sparkContext=sc)

print "Setting LOG LEVEL as ERROR"
sc.setLogLevel("ERROR")

print "Add cso.zip to system path"
import sys

sys.path.insert(0, "cso.zip")

# import time
# start_time = time.time()

ORDER_DATES = get_order_dates_between(start_date=START_DATE_ORDER, end_date=END_DATE_ORDER)

for order_date in ORDER_DATES:
    vl_df = _get_visit_list(sc=sc, sqlContext=sqlContext, order_date=order_date)
    aglu_df = _get_aglu_list(sc=sc, sqlContext=sqlContext)

    vl_df.select('KUNNR', 'del_date').registerTempTable("vl_sql")
    aglu_df.registerTempTable("aglu_sql")

    q = """
    select e.*, f.matnr matnr
    from
    (
    select b.kunnr customernumber, b.del_date del_date, d.scl_auth_matlst scl_auth_matlst, d.vkbur vkbur
    from
    (
    select a.KUNNR kunnr, a.del_date del_date
    from vl_sql a
    where a.KUNNR in ('0500066337','0500070166','0500070167','0500075749','0500083147','0500061438','0500067084','0500058324','0500080723','0500060033','0500068825','0500060917','0500078551','0500076115','0500071747','0500078478','0500078038','0500073982','0500064458','0500268924','0500070702','0500070336','0500076032','0500095883','0500284889')
    ) b
    join
    (
    select c.kunnr kunnr, c.scl_auth_matlst scl_auth_matlst, c.vkbur vkbur
    from mdm.customer c
    ) d
    on d.kunnr = b.kunnr
    ) e
    cross join
    (
    select g.MATNR matnr, g.SCL_AUTH_MATLST scl_auth_matlst
    from aglu_sql g
    ) f
    where e.scl_auth_matlst = f.scl_auth_matlst
    """

    visit_list_final = sqlContext.sql(q)

    invoice_raw = _get_invoice_data(sqlContext=sqlContext, CRITERIA_DATE=get_criteria_date(order_date=order_date))

    visit_invoice_condition = [visit_list_final.customernumber == invoice_raw.customernumber,
                               visit_list_final.matnr == invoice_raw.matnr]

    visit_list_final_join_invoice_raw = visit_list_final \
        .join(invoice_raw, on=visit_invoice_condition, how='inner') \
        .select(visit_list_final.customernumber,
                visit_list_final.matnr,
                visit_list_final.del_date,
                visit_list_final.scl_auth_matlst,
                visit_list_final.vkbur,
                invoice_raw.bill_date)

    prediction_data = _get_prediction_list(sc=sc, sqlContext=sqlContext)

    visit_pred_condition = [visit_list_final_join_invoice_raw.customernumber == prediction_data.customernumber,
                            visit_list_final_join_invoice_raw.matnr == prediction_data.matnr]

    final_df = visit_list_final_join_invoice_raw \
        .join(prediction_data, on=visit_pred_condition, how='inner') \
        .drop(prediction_data.customernumber) \
        .drop(prediction_data.matnr) \
        .withColumn('last_del_date', from_unixtime(unix_timestamp(col('bill_date'), "yyyyMMdd")).cast(DateType())) \
        .drop(col('bill_date')) \
        .withColumn('_diff_day', datediff(col('del_date'), col('last_del_date'))) \
        .withColumn('quantity',
                    when(col('pdt_cat') == 'I', round((col('pred_val') / 7) * col('_diff_day')))
                    .when(col('pdt_cat') == 'II', round((col('pred_val') / 7) * col('_diff_day')))
                    .when(col('pdt_cat') == 'III', round((col('pred_val') / 7) * col('_diff_day')))
                    .when(col('pdt_cat') == 'VII', round((col('pred_val') / 7) * col('_diff_day')))
                    .otherwise(round((col('pred_val') / 31) * col('_diff_day')))) \
        .filter(col('quantity') > 0) \
        .repartition(10)

    final_df.coalesce(1) \
        .write.mode('append') \
        .format('orc') \
        .option("header", "false") \
        .save(FINAL_PREDICTION_LOCATION)


# print("Time taken for execution:\t\t--- %s seconds ---" % (time.time() - start_time))
