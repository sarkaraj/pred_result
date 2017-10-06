from properties import conversion_denominator


# from properties import START_DATE_ORDER, END_DATE_ORDER


def _linear_scaling(diff, category, pred_val):
    denom = conversion_denominator.get(category)
    return round(float(pred_val / denom) * float(diff), 2)


def get_current_date():
    import datetime

    _date = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")

    return _date


def get_order_dates_between(start_date, end_date, **kwargs):
    import numpy as np
    _result = np.arange(start_date, end_date, dtype='datetime64[D]')
    return _result[np.is_busday(_result)]


def get_criteria_date(order_date):
    import numpy as np
    import pandas as pd

    a = np.busday_offset(order_date, 2, roll='forward')
    t = pd.to_datetime(str(a))
    b = t.strftime('\'%Y%m%d\'')
    return b


if __name__ == "__main__":

    order_dates = get_order_dates_between(start_date=START_DATE_ORDER, end_date=END_DATE_ORDER)
    print order_dates

    for order_date in order_dates:
        print order_date
        print get_criteria_date(order_date=order_date)
