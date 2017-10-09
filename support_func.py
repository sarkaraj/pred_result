from properties import pdt_cat_denominator
from pyspark.sql.functions import udf
from pyspark.sql.types import *
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

    a = np.busday_offset(order_date, 2, roll='forward')
    t = pd.to_datetime(str(a))
    b = t.strftime('\'%Y%m%d\'')
    return b


def get_delivery_date_from_order_date(order_date, delivery_lag):
    import numpy as np
    _result = str(np.busday_offset(order_date, delivery_lag, roll='forward'))
    return _result

# Creating UDF to get delivery_date from order_date based on delivery_lag
udf_get_delivery_date_from_order_date = udf(get_delivery_date_from_order_date, StringType())


def linear_scale(pdt_cat, pred_val, _diff_day):
    denom = pdt_cat_denominator.get(pdt_cat)
    _result = round((pred_val / denom) * _diff_day)
    return _result

# Creating UDF to linearly scale prediction value based on pdt_cat_denominator
udf_linear_scale = udf(linear_scale, DoubleType())


if __name__ == "__main__":

    order_dates = get_order_dates_between(start_date=START_DATE_ORDER, end_date=END_DATE_ORDER)
    print order_dates

    for order_date in order_dates:
        print type(order_date)
        print get_criteria_date(order_date=order_date)
