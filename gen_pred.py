"""

"""
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from support_func import _generate_invoice, get_order_dates_between
from transform.support_func import _get_visit_list_from_invoice
from support_func import get_current_date
from properties import *


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

# Obtain visit list from invoice data. TODO : This will be replaced.
_visit_list = _get_visit_list_from_invoice(sqlContext=sqlContext, start_date=START_DATE_INVOICE,
                                           end_date=END_DATE_INVOICE)

_visit_list.cache()

# Get all order dates between START_DATE_ORDER and END_DATE_ORDER
ORDER_DATES = get_order_dates_between(start_date=START_DATE_ORDER, end_date=END_DATE_ORDER)

# ORDER_DATES = ['2017-09-06', '2017-09-13']

# # For testing
# # order_date = ORDER_DATES[1]

# Loop through all order dates to generate invoices of each date.
for order_date in ORDER_DATES:
    print("************************************************************************************")
    print ("Generating Invoice for " + order_date)
    print("************************************************************************************")
    _generate_invoice(sc=sc, sqlContext=sqlContext, visit_list=_visit_list, order_date=order_date)
    print("************************************************************************************")

# Clearing Cache
sqlContext.clearCache()
# Stopping SparkContext
sc.stop()
