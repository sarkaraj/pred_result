from properties import pdt_cat_denominator
from transform._visit_list import _get_visit_list
from transform._aglu_list import _get_aglu_list
import numpy as np
from properties import START_DATE_ORDER, END_DATE_ORDER


def _linear_scaling(diff, category, pred_val):
    denom = pdt_cat_denominator.get(category)
    return round(float(pred_val / denom) * float(diff), 2)


def get_current_date():
    import datetime

    _date = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")

    return _date


def get_order_dates_between(start_date, end_date, **kwargs):
    import numpy as np
    _result = np.arange(start_date, end_date, dtype='datetime64[D]')
    _result_final = [str(_date) for _date in _result[np.is_busday(_result)]]
    return _result_final


def get_criteria_date(order_date, **kwargs):
    import numpy as np
    import pandas as pd

    # # TODO Needs clarification about what logic to put
    # a = np.busday_offset(order_date, 2, roll='forward')
    a = order_date # # as of now this is a redundant line
    t = pd.to_datetime(str(a))
    b = t.strftime('\'%Y%m%d\'')
    return b


def get_delivery_date_from_order_date(order_date, delivery_lag):
    import numpy as np
    _result = str(np.busday_offset(order_date, delivery_lag, roll='forward'))
    return _result

# Creating UDF to get delivery_date from order_date based on delivery_lag
# udf_get_delivery_date_from_order_date = udf(get_delivery_date_from_order_date, StringType())


def linear_scale(pdt_cat, pred_val, _diff_day):
    denom = pdt_cat_denominator.get(pdt_cat)
    _result = round((pred_val / denom) * _diff_day)
    return _result


def _get_delivery_date(date, lag):
    _date = str(date)
    _lag = int(lag)
    return str(np.busday_offset(_date, _lag, roll='forward'))


def _generate_invoice(sc, sqlContext, **kwargs):
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from transform._invoice_latest import _get_invoice_data
    from transform._prediction_list import _get_prediction_list


    _visit_list = kwargs.get('visit_list')

    ORDER_DATES = get_order_dates_between(start_date=START_DATE_ORDER, end_date=END_DATE_ORDER)
    order_date = ORDER_DATES[0]
    vl_df = _get_visit_list(sc=sc, sqlContext=sqlContext, order_date=order_date,
                            visit_list=_visit_list)  # # Gets the visit list for a given order date
    aglu_df = _get_aglu_list(sc=sc, sqlContext=sqlContext)

    # vl_df.show(10)
    # aglu_df.show(10)

    # for order_date in ORDER_DATES:
    vl_df = _get_visit_list(sc=sc, sqlContext=sqlContext, order_date=order_date,
                            visit_list=_visit_list)  # # Gets the visit list for a given order date
    aglu_df = _get_aglu_list(sc=sc, sqlContext=sqlContext)

    vl_df.registerTempTable("vl_sql")
    aglu_df.select(col('MATNR').alias('mat_no'), col('scl_auth_matlst')).registerTempTable("aglu_sql")

    q = """
    select e.*, f.mat_no mat_no
    from
    (
    select b.customernumber customernumber, b.order_date order_date, d.scl_auth_matlst scl_auth_matlst, d.vkbur vkbur, IF(d.vsbed == '01', 2, 1) dlvry_lag
    from
    (
    select a.customernumber customernumber, a.order_date order_date
    from vl_sql a
    ) b
    join
    (
    select c.kunnr customernumber, c.scl_auth_matlst scl_auth_matlst, c.vkbur vkbur, c.vsbed vsbed
    from mdm.customer c
    ) d
    on d.customernumber = b.customernumber
    ) e
    cross join
    (
    select g.mat_no mat_no, g.SCL_AUTH_MATLST scl_auth_matlst
    from aglu_sql g
    ) f
    where e.scl_auth_matlst = f.scl_auth_matlst
    """

    # # Obtaining delivery_date from given order_date provided the delivery_lag
    # TODO Start from this point
    visit_list_final = sqlContext.sql(q) \
        .drop(col('scl_auth_matlst')) \
        .withColumn('delivery_date', udf(_get_delivery_date, StringType())(col('order_date'), col('dlvry_lag'))) \
        .drop(col('dlvry_lag'))

    visit_list_final.show()
    # visit_list_final.printSchema()

    invoice_raw = _get_invoice_data(sqlContext=sqlContext, CRITERIA_DATE=get_criteria_date(order_date=order_date)) #TODO Check viability

    invoice_raw.show()


    visit_invoice_condition = [visit_list_final.customernumber == invoice_raw.customernumber,
                                   visit_list_final.mat_no == invoice_raw.mat_no]

    visit_list_final_join_invoice_raw = visit_list_final \
        .join(invoice_raw, on=visit_invoice_condition, how='inner') \
        .select(visit_list_final.customernumber,
                visit_list_final.mat_no,
                visit_list_final.order_date,
                visit_list_final.vkbur,
                visit_list_final.delivery_date,
                invoice_raw.last_delivery_date)

    visit_list_final_join_invoice_raw.show()
    # print visit_list_final_join_invoice_raw.count()

    prediction_data = _get_prediction_list(sc=sc, sqlContext=sqlContext, CRITERIA_DATE=order_date)


    visit_pred_condition = [visit_list_final_join_invoice_raw.customernumber == prediction_data.customernumber,
                            visit_list_final_join_invoice_raw.mat_no == prediction_data.mat_no]


    final_df = visit_list_final_join_invoice_raw \
        .join(prediction_data, on=visit_pred_condition, how='inner')
    #         .drop(prediction_data.customernumber) \
    #         .drop(prediction_data.matnr) \
    #         .withColumn('last_del_date', from_unixtime(unix_timestamp(col('bill_date'), "yyyyMMdd")).cast(DateType())) \
    #         .drop(col('bill_date')) \
    #         .withColumn('_diff_day', datediff(col('del_date'), col('last_del_date'))) \
    #         .withColumn('quantity', udf_linear_scale(col('pdt_cat'), col('pred_val'), col('_diff_day'))) \
    #         .filter(col('quantity') > 0) \
    #         .repartition(10)

    final_df.show()
    #
    #     final_df.coalesce(1) \
    #         .write.mode('append') \
    #         .format('orc') \
    #         .option("header", "false") \
    #         .save(FINAL_PREDICTION_LOCATION)


if __name__ == "__main__":

    order_dates = get_order_dates_between(start_date=START_DATE_ORDER, end_date=END_DATE_ORDER)
    print order_dates

    # for order_date in order_dates:
    #     print type(order_date)
    #     print get_criteria_date(order_date=order_date)
