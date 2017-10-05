from properties import conversion_denominator


def _linear_scaling(diff, category, pred_val):
    denom = conversion_denominator.get(category)
    return round(float(pred_val / denom) * float(diff), 2)


def get_current_date():
    import datetime

    _date = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")

    return _date
