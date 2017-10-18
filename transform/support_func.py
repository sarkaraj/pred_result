from properties import invoice_q
import numpy as np


def string_to_gregorian(dt_str, sep='-', **kwargs):
    from datetime import date
    x = dt_str.split(sep)
    if (isinstance(x[0], int) and isinstance(x[1], int) and isinstance(x[2], int)):
        year = x[0]
        month = x[1]
        day = x[2]
    else:
        year = int(x[0])
        month = int(x[1])
        day = int(x[2])

    return date(year=year, month=month, day=day)


def _generate_invoice_query_btwn_dt(start_date, end_date, **kwargs):
    '''
    Generates query to obtain invoices between two given dates
    :param start_date: datetime.date object or str object of format YYYY-MM-DD. Date is inclusive
    :param end_date: datetime.date object or str object of format YYYY-MM-DD. Date is inclusive
    :return: full query as a single string
    '''
    import datetime

    if (isinstance(start_date, datetime.date)):
        from_date = start_date.strftime('\'%Y%m%d\'')
    elif (isinstance(start_date, str)):
        from_date = string_to_gregorian(start_date).strftime('\'%Y%m%d\'')
    else:
        raise TypeError

    if (isinstance(end_date, datetime.date)):
        to_date = end_date.strftime('\'%Y%m%d\'')
    elif (isinstance(end_date, str)):
        to_date = string_to_gregorian(end_date).strftime('\'%Y%m%d\'')
    else:
        raise TypeError

    bill_date = "d.bill_date"
    gte = ">="
    _and = "and"
    lte = "<="
    _q_subpart = " ".join([bill_date, gte, from_date, _and, bill_date, lte, to_date])

    _result = " ".join([invoice_q, _q_subpart])

    return _result


def _get_dlvry_dt_frm_b_date(x):
    return x.strftime('%Y-%m-%d')


def _get_bus_day_flag(x):
    return str(np.is_busday([x])[0])


def _get_visit_date(x, y):
    return str(np.busday_offset(x, -y, roll='backward'))


def _get_visit_list_from_invoice(sqlContext, start_date, end_date, **kwargs):
    '''

    :param sqlContext: SQLContext for running query on Hive
    :param start_date: start date of invoice. datetime.date object or str object of format YYYY-MM-DD. Date is inclusive
    :param end_date: end date of invoice. datetime.date object or str object of format YYYY-MM-DD. Date is inclusive
    :param kwargs: None as of 18-10-2017
    :return: Spark Dataframe of visit list between start_date and end_date of unique customers
    '''
    from pyspark.sql.functions import *
    from pyspark.sql.types import *

    _q = _generate_invoice_query_btwn_dt(start_date=start_date, end_date=end_date)

    _visit_list = sqlContext.sql(_q) \
        .filter(col('quantity') > 0) \
        .withColumn('b_date', from_unixtime(unix_timestamp(col('bill_date'), "yyyyMMdd")).cast(DateType())) \
        .withColumn('dlvry_date', udf(_get_dlvry_dt_frm_b_date, StringType())(col('b_date'))) \
        .withColumn('bus_day_flag', udf(_get_bus_day_flag, StringType())(col('dlvry_date')).cast(BooleanType())) \
        .withColumn('order_date',
                    udf(_get_visit_date, StringType())(col('dlvry_date'), col('dlvry_lag')).cast(DateType())) \
        .select(col('customernumber'), col('order_date')) \
        .distinct()

    return _visit_list


if __name__ == "__main__":
    # from datetime import date
    #
    # # print invoice_q
    # # d.bill_date >= '20170903' and d.bill_date <= '20171007'
    # start_date = date(year=2017, month=10, day=18)
    # end_date = date(year=2017, month=10, day=20)
    print _generate_invoice_query_btwn_dt('2017-10-18', '2017-10-20')
    # print _generate_invoice_query_btwn_dt(start_date, end_date)
