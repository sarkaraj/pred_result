from pyspark.sql.functions import *
from pyspark.sql.types import *


def _get_remainder(sqlContext, **kwargs):
    from properties import TABLE_CSO_PREDICTION
    testing = kwargs.get('testing')

    if testing:
        _q = """select b.customernumber customernumber, b.mat_no mat_no, b.remainder remainder
                from
                (
                select customernumber, mat_no, remainder, to_date(from_unixtime(unix_timestamp(order_date, 'yyyy-MM-dd'))) order_date
                from predicted_order.cso_prediction_model_eda
                ) b
                join
                (
                select customernumber, mat_no, max(to_date(from_unixtime(unix_timestamp(order_date, 'yyyy-MM-dd')))) lst_order_dt
                from predicted_order.cso_prediction_model_eda
                group by customernumber, mat_no
                ) a
                on a.customernumber=b.customernumber and a.mat_no=b.mat_no and a.lst_order_dt=b.order_date"""
    else:
        _q = """select b.customernumber customernumber, b.mat_no mat_no, b.remainder remainder
                from
                (
                select customernumber, mat_no, remainder, to_date(from_unixtime(unix_timestamp(order_date, 'yyyy-MM-dd'))) order_date
                from """ + TABLE_CSO_PREDICTION + \
             """) b
             join
             (
             select customernumber, mat_no, max(to_date(from_unixtime(unix_timestamp(order_date, 'yyyy-MM-dd')))) lst_order_dt
             from """ + TABLE_CSO_PREDICTION + \
             """ group by customernumber, mat_no
             ) a
             on a.customernumber=b.customernumber and a.mat_no=b.mat_no and a.lst_order_dt=b.order_date"""

    _remainder_df = sqlContext.sql(_q)

    return _remainder_df

    # predicted_order.cso_prediction_model_eda
