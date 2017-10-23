from properties import CUSTOMER_LIST
from support_func import _get_dt_frm_b_date
from pyspark.sql.functions import *
from pyspark.sql.types import *

def _get_invoice_data(sqlContext, **kwargs):
    '''
    Gets latest invoice data for all customers
    :param sqlContext: SQLContext
    :param kwargs: Used for 'CRITERIA_DATE' -->
    :return:
    '''

    CRITERIA_DATE = kwargs.get('CRITERIA_DATE')
    q_2 = """
    select a.kunag customernumber, a.matnr mat_no, max(a.fkdat) bill_date
    from skuopt.invoices a
    where a.kunag in"""
    cust_list = CUSTOMER_LIST

    _condition = """and a.fkdat <= """ + CRITERIA_DATE + """ group by a.kunag,a.matnr"""

    _query = " ".join([q_2, cust_list, _condition])

    _invoice_raw_data = sqlContext.sql(_query) \
        .withColumn('b_date', from_unixtime(unix_timestamp(col('bill_date'), "yyyyMMdd")).cast(DateType())) \
        .withColumn('last_delivery_date', udf(_get_dt_frm_b_date, StringType())(col('b_date'))) \
        .drop(col('b_date')) \
        .drop(col('bill_date'))

    return _invoice_raw_data


if __name__ == "__main__":
    # _get_invoice_data()

    q_2 = """
        select a.kunag customernumber, a.matnr matnr, max(a.fkdat) last_delivery_date
        from skuopt.invoices a
        where a.kunag in"""
    cust_list = CUSTOMER_LIST

    _condition = """and a.fkdat <= """ + "blah" + """ group by a.kunag,a.matnr"""

    _query = " ".join([q_2, cust_list, _condition])

    print _query
