from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from support_func import _generate_invoice
from transform.support_func import _get_visit_list_from_invoice
from support_func import \
    get_current_date  # , get_order_dates_between #, get_criteria_date, udf_get_delivery_date_from_order_date, udf_linear_scale
from properties import START_DATE_INVOICE, END_DATE_INVOICE


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

_visit_list = _get_visit_list_from_invoice(sqlContext=sqlContext, start_date=START_DATE_INVOICE,
                                           end_date=END_DATE_INVOICE)
# _visit_list.show()

_generate_invoice(sc=sc, sqlContext=sqlContext, visit_list=_visit_list)

sc.stop()
