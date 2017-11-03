from pyspark.sql.functions import *
from pyspark.sql.types import *
from properties import pdt_cat_denominator
from transform._visit_list import _get_visit_list
# from transform._aglu_list import _get_aglu_list
import numpy as np
from properties import START_DATE_ORDER, END_DATE_ORDER
from datetime import datetime
import datetime


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


def _generate_dates_between(start_date, end_date, **kwargs):
    import numpy as np
    _start_date = (string_to_gregorian(start_date) + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    _end_date = (string_to_gregorian(end_date) + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    _result = np.arange(_start_date, _end_date, dtype='datetime64[D]')
    _result_final = [str(_date) for _date in _result]
    return set(_result_final)


def iso_year_start(iso_year):
    "The gregorian calendar date of the first day of the given ISO year"
    fourth_jan = datetime.date(iso_year, 1, 4)
    delta = datetime.timedelta(fourth_jan.isoweekday() - 1)
    return fourth_jan - delta


def iso_to_gregorian(iso_year, iso_week, iso_day):
    """
    Converts ISO Calendar to Gregorian Date
    Gregorian calendar date for the given ISO year, week and day
    :param iso_year: ISO Year
    :param iso_week: ISO Week Number
    :param iso_day: ISO Day of Week
    :return: Gregorian Date Object
    """
    year_start = iso_year_start(iso_year)
    return year_start + datetime.timedelta(days=iso_day - 1, weeks=iso_week - 1)


def _generate_dates_from_week(week_num_year):
    _week_num, _year = (int(elem) for elem in week_num_year.split("-"))
    week_start_day = 1
    week_end_day = 7
    # _week_num = int(week_num)
    # _year = int(year)
    _result = [iso_to_gregorian(iso_year=_year, iso_week=_week_num, iso_day=day).strftime('%Y-%m-%d') for day in
               range(week_start_day, week_end_day + 1)]
    return set(_result)


def _generate_dates_from_month(month_year):
    import calendar
    _month, _year = (int(elem) for elem in month_year.split("-"))
    num_days = calendar.monthrange(year=_year, month=_month)[1]
    days = [datetime.date(_year, _month, _day).strftime('%Y-%m-%d') for _day in range(1, num_days + 1)]
    return set(days)


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


def _get_tweaked_last_del_dt(input_date, **kwargs):
    '''
    If last_delivery_date is < _SIMULATION_START_DATE then last_delivery_date = _SIMULATION_START_DATE
    :param input_date: last_delivery_date
    :param kwargs: SIMULATION_START_DATE from properties file
    :return: tweaked_last_delivery_date : String of format 'yyyy-MM-dd'
    '''
    from properties import SIMULATION_START_DATE
    from transform.support_func import string_to_gregorian

    if 'SIMULATION_START_DATE' in kwargs.keys():
        _SIMULATION_START_DATE = kwargs.get('SIMULATION_START_DATE')
    else:
        _SIMULATION_START_DATE = SIMULATION_START_DATE  # # SIMULATION_START_DATE is a string of format 'yyyy-MM-dd'

    _input_date = string_to_gregorian(input_date)
    _check_date = string_to_gregorian(_SIMULATION_START_DATE)

    if (_input_date < _check_date):
        return _check_date.strftime('%Y-%m-%d')
    else:
        return _input_date.strftime('%Y-%m-%d')


def _is_model_valid(delivery_dt, lst_delivery_dt, mdl_bld_dt, min_mdl_bld_dt):
    from transform.support_func import string_to_gregorian

    _delivery_dt = string_to_gregorian(delivery_dt)
    _lst_delivery_dt = string_to_gregorian(lst_delivery_dt)
    _mdl_bld_dt = string_to_gregorian(mdl_bld_dt)
    _min_mdl_bld_dt = string_to_gregorian(min_mdl_bld_dt)

    if _mdl_bld_dt <= _delivery_dt and _mdl_bld_dt >= _lst_delivery_dt:
        return True
    elif _mdl_bld_dt < _lst_delivery_dt:
        if _mdl_bld_dt == _min_mdl_bld_dt:
            return True
        else:
            return False


def _get_max_date_spprt_func_1(mdl_bld_dt, mod_last_delivery_date):
    from transform.support_func import string_to_gregorian

    _mdl_bld_dt = string_to_gregorian(mdl_bld_dt)
    _mod_last_delivery_date = string_to_gregorian(mod_last_delivery_date)

    if (_mdl_bld_dt <= _mod_last_delivery_date):
        return _mdl_bld_dt
    else:
        return string_to_gregorian('1970-01-01')


def map_pred_val_to_week_month_year(row_object, **kwargs):
    customernumber = row_object.customernumber
    mat_no = row_object.mat_no
    pred_val_dict = row_object.pred_val
    pdt_cat = row_object.pdt_cat
    order_date = row_object.order_date
    delivery_date = row_object.delivery_date
    mod_last_delivery_date = row_object.mod_last_delivery_date
    mdl_bld_dt = row_object.mdl_bld_dt
    scale_denom = row_object.scale_denom

    # _result = (customernumber, mat_no, pred_val_dict, pdt_cat, order_date, delivery_date, mod_last_delivery_date, mdl_bld_dt, scale_denom)
    _result = [(customernumber, mat_no, key, float(pred_val_dict.get(key)), pdt_cat, order_date, delivery_date,
                mod_last_delivery_date, mdl_bld_dt, float(scale_denom)) for key in pred_val_dict.keys()]

    return _result


def _pred_val_to_week_month_year_schema():
    customernumber = StructField("customernumber", StringType(), nullable=True)
    mat_no = StructField("mat_no", StringType(), nullable=True)
    week_month_key = StructField("week_month_key", StringType(), nullable=True)
    pred_val = StructField("pred_val", FloatType(), nullable=True)
    pdt_cat = StructField("pdt_cat", StringType(), nullable=True)
    order_date = StructField("order_date", StringType(), nullable=True)
    delivery_date = StructField("delivery_date", StringType(), nullable=True)
    mod_last_delivery_date = StructField("mod_last_delivery_date", StringType(), nullable=True)
    mdl_bld_dt = StructField("mdl_bld_dt", StringType(), nullable=True)
    scale_denom = StructField("scale_denom", FloatType(), nullable=True)

    _schema = StructType(
        [customernumber, mat_no, week_month_key, pred_val, pdt_cat, order_date, delivery_date, mod_last_delivery_date,
         mdl_bld_dt, scale_denom])

    return _schema


def _get_partial_date_diff(week_month_key, pdt_cat, delivery_date, last_delivery_date):
    if (pdt_cat in ('I', 'II', 'III', 'VII')):
        _model_type = 'W'
    else:
        _model_type = 'M'

    if (_model_type == 'W'):
        _days = _generate_dates_between(start_date=last_delivery_date, end_date=delivery_date)
        _days_week = _generate_dates_from_week(week_month_key)
        _partial_days = _days_week.intersection(_days)
        return len(_partial_days)

    elif (_model_type == 'M'):
        _days = _generate_dates_between(start_date=last_delivery_date, end_date=delivery_date)
        _days_month = _generate_dates_from_month(week_month_key)
        _partial_days = _days_month.intersection(_days)
        return len(_partial_days)


def _calculate_quantity(pred_val, scale_denom, partial_diff):
    _pred_val = float(pred_val)
    _scale_denom = float(scale_denom)
    _partial_diff = float(partial_diff)

    _result = round(((_pred_val / _scale_denom) * _partial_diff), 2)
    return _result

def _generate_invoice(sc, sqlContext, **kwargs):
    from transform._invoice_latest import _get_invoice_data
    from transform._prediction_list import _get_models_list, _get_prediction_list
    from properties import FINAL_PREDICTION_LOCATION, _PREDICTION_LOCATION, TESTING
    from transform._remainder_addition import _get_remainder

    # Get visit list as an argument
    _visit_list = kwargs.get('visit_list')

    # Get order date as an argument -- Used for simulation purpose now. Should be changed to current system date ideally
    order_date = kwargs.get('order_date')

    # Filters out the visit list for the provided order_date
    vl_df = _get_visit_list(sc=sc, sqlContext=sqlContext, order_date=order_date,
                            visit_list=_visit_list)  # # Gets the visit list for a given order date

    # # Used to the authorised material list for a particular 'vkbur'
    # aglu_df = _get_aglu_list(sc=sc, sqlContext=sqlContext)


    vl_df.registerTempTable("vl_sql")
    # # query to join the visit list with authorised material list to obtain a complete set of products for all scheduled visits.
    # # TODO: Clarify is there is any method to isolate the materials for a particular visit

    # aglu_df.select(col('MATNR').alias('mat_no'), col('scl_auth_matlst')).registerTempTable("aglu_sql")

    # q = """
    # select e.*, f.mat_no mat_no
    # from
    # (
    # select b.customernumber customernumber, b.order_date order_date, d.scl_auth_matlst scl_auth_matlst, d.vkbur vkbur, IF(d.vsbed == '01', 2, 1) dlvry_lag
    # from
    # (
    # select a.customernumber customernumber, a.order_date order_date
    # from vl_sql a
    # ) b
    # join
    # (
    # select c.kunnr customernumber, c.scl_auth_matlst scl_auth_matlst, c.vkbur vkbur, c.vsbed vsbed
    # from mdm.customer c
    # ) d
    # on d.customernumber = b.customernumber
    # ) e
    # cross join
    # (
    # select g.mat_no mat_no, g.SCL_AUTH_MATLST scl_auth_matlst
    # from aglu_sql g
    # ) f
    # where e.scl_auth_matlst = f.scl_auth_matlst
    # """

    # Query to join the visit list to the customer master table to obtain delivery lag for a customer
    q = """
    select e.*
    from
    (
    select b.customernumber customernumber, b.mat_no mat_no, b.order_date order_date, d.scl_auth_matlst scl_auth_matlst, d.vkbur vkbur, IF(d.vsbed == '01', 2, 1) dlvry_lag
    from
    (
    select a.customernumber customernumber, a.mat_no mat_no, a.order_date order_date
    from vl_sql a
    ) b
    join
    (
    select c.kunnr customernumber, c.scl_auth_matlst scl_auth_matlst, c.vkbur vkbur, c.vsbed vsbed
    from mdm.customer c
    ) d
    on d.customernumber = b.customernumber
    ) e
    """

    # # Obtaining delivery_date from given order_date provided the delivery_lag
    visit_list_final = sqlContext.sql(q) \
        .drop(col('scl_auth_matlst')) \
        .withColumn('delivery_date', udf(_get_delivery_date, StringType())(col('order_date'), col('dlvry_lag'))) \
        .drop(col('dlvry_lag')) \
        .repartition(60)

    print("Visit List Final")

    #
    invoice_raw = _get_invoice_data(sqlContext=sqlContext,
                                    CRITERIA_DATE=get_criteria_date(order_date=order_date)).repartition(60)

    print("Invoice_raw")
    # invoice_raw.filter(col('customernumber').like('0500064458') & col('mat_no').like('000000000000119826')).show()


    visit_invoice_condition = [visit_list_final.customernumber == invoice_raw.customernumber,
                                   visit_list_final.mat_no == invoice_raw.mat_no]

    visit_list_final_join_invoice_raw = visit_list_final \
        .join(invoice_raw, on=visit_invoice_condition, how='inner') \
        .select(visit_list_final.customernumber,
                visit_list_final.mat_no,
                visit_list_final.order_date,
                visit_list_final.vkbur,
                visit_list_final.delivery_date,
                invoice_raw.last_delivery_date) \
        .withColumn('mod_last_delivery_date', udf(_get_tweaked_last_del_dt, StringType())(col('last_delivery_date'))) \
        .drop(col('last_delivery_date')) \
        .repartition(60)


    print("Visit list final join invoice raw")
    # visit_list_final_join_invoice_raw.filter(col('customernumber').like('0500064458') & col('mat_no').like('000000000000119826')).show()
    # print(visit_list_final_join_invoice_raw.count())
    # visit_list_final_join_invoice_raw.printSchema()

    models_data_raw = _get_models_list(sc=sc, sqlContext=sqlContext, CRITERIA_DATE=order_date,
                                       testing=TESTING).repartition(60)
    print("Prediction Data")
    # models_data_raw.filter(col('customernumber').like('0500064458') & col('mat_no').like('000000000000119826')).show()
    # print(models_data_raw.count())
    # models_data_raw.printSchema()


    visit_pred_condition = [visit_list_final_join_invoice_raw.customernumber == models_data_raw.customernumber,
                            visit_list_final_join_invoice_raw.mat_no == models_data_raw.mat_no]

    _final_df_stage = visit_list_final_join_invoice_raw \
        .join(models_data_raw, on=visit_pred_condition, how='inner') \
        .select(visit_list_final_join_invoice_raw.customernumber,
                visit_list_final_join_invoice_raw.mat_no,
                visit_list_final_join_invoice_raw.order_date,
                visit_list_final_join_invoice_raw.vkbur,
                visit_list_final_join_invoice_raw.delivery_date,
                visit_list_final_join_invoice_raw.mod_last_delivery_date,
                models_data_raw.mdl_bld_dt
                ).repartition(60)

    print("Final_df_stage")
    # _final_df_stage.filter(col('customernumber').like('0500064458') & col('mat_no').like('000000000000119826')).show()

    ########################################
    from pyspark.sql.window import Window
    import sys
    ########################################

    cust_pdt_mdl_bld_dt_window = Window \
        .partitionBy(_final_df_stage.customernumber, _final_df_stage.mat_no) \
        .orderBy(_final_df_stage.mdl_bld_dt) \
        .rangeBetween(-sys.maxsize, sys.maxsize)

    # valid_model_flag = (udf(_is_model_valid, BooleanType())(col('delivery_date'), col('mod_last_delivery_date'), col('mdl_bld_dt'), max(col('mdl_bld_dt').filter(lambda _date: _date < col('mod_last_delivery_date'))).alias('min_mdl_bld_dt')))
    _final_df = _final_df_stage \
        .select(col('customernumber'),
                col('mat_no'),
                col('order_date'),
                col('vkbur'),
                col('delivery_date'),
                col('mod_last_delivery_date'),
                col('mdl_bld_dt'),
                (max(udf(_get_max_date_spprt_func_1, DateType())(col('mdl_bld_dt'),
                                                                 col('mod_last_delivery_date')))).over(
                    window=cust_pdt_mdl_bld_dt_window).cast(StringType()).alias('min_mdl_bld_dt')
                ) \
        .withColumn('mdl_validity',
                    udf(_is_model_valid, BooleanType())(col('delivery_date'), col('mod_last_delivery_date'),
                                                        col('mdl_bld_dt'), col('min_mdl_bld_dt'))) \
        .filter(col('mdl_validity') == True) \
        .repartition(60)

    print("Final_df")
    # _final_df.filter(col('customernumber').like('0500064458') & col('mat_no').like('000000000000119826')).show()

    _prediction_df_raw = _get_prediction_list(sqlContext=sqlContext, testing=TESTING).repartition(60)

    print ("prediction_df_raw")
    # _prediction_df_raw.filter(col('customernumber').like('0500064458') & col('mat_no').like('000000000000119826')).show()

    _final_df_prediction_df_raw_condition = [_final_df.customernumber == _prediction_df_raw.customernumber,
                                             _final_df.mat_no == _prediction_df_raw.mat_no,
                                             _final_df.mdl_bld_dt == _prediction_df_raw.mdl_bld_dt]

    _temp_df = _prediction_df_raw \
        .join(_final_df, on=_final_df_prediction_df_raw_condition, how='inner') \
        .select(_prediction_df_raw.customernumber,
                _prediction_df_raw.mat_no,
                _prediction_df_raw.pred_val,
                _prediction_df_raw.pdt_cat,
                _final_df.order_date.cast(StringType()),
                _final_df.delivery_date,
                _final_df.mod_last_delivery_date,
                _final_df.mdl_bld_dt
                ) \
        .withColumn('scale_denom', when(col('pdt_cat').isin('I', 'II', 'III', 'VII'), 7).otherwise(31)) \
        .repartition(60)

    print("_temp_df")
    # _temp_df.filter(col('customernumber').like('0500064458') & col('mat_no').like('000000000000119826')).show()
    # _temp_df.printSchema()

    _temp_df_rdd_mapped = _temp_df.flatMap(lambda _row: map_pred_val_to_week_month_year(_row))
    # _temp_df_rdd_mapped = _temp_df.map(lambda _row: _row)

    # print _temp_df_rdd_mapped.take(1)

    # cust_pdt_week_month_window = Window \
    #     .partitionBy(_final_df_stage.customernumber, _final_df_stage.mat_no) \
    #     .orderBy(_final_df_stage.mdl_bld_dt) \
    #     .rangeBetween(-sys.maxsize, sys.maxsize)

    _temp_df_flat = sqlContext.createDataFrame(_temp_df_rdd_mapped, schema=_pred_val_to_week_month_year_schema()) \
        .select(col('customernumber'),
                col('mat_no'),
                col('week_month_key'),
                col('pred_val'),
                col('pdt_cat'),
                col('delivery_date'),
                col('mod_last_delivery_date').alias('last_delivery_date'),
                col('mdl_bld_dt'),
                col('scale_denom'),
                (max(udf(string_to_gregorian, DateType())(col('mdl_bld_dt')))).over(
                    window=Window.partitionBy("customernumber", "mat_no", "week_month_key").orderBy(
                        "mdl_bld_dt").rangeBetween(-sys.maxsize, sys.maxsize)).cast(
                    StringType()).alias('updt_mdl_bld_dt')
                ) \
        .filter(col('mdl_bld_dt') == col('updt_mdl_bld_dt')) \
        .withColumn('_partial_diff', udf(_get_partial_date_diff, IntegerType())(col('week_month_key'), col('pdt_cat'),
                                                                                col('delivery_date'),
                                                                                col('last_delivery_date'))) \
        .withColumn('_partial_quantity_temp', (col('pred_val') / col('scale_denom')) * col('_partial_diff')) \
        .withColumn('_partial_quantity', round(col('_partial_quantity_temp'), 2)) \
        .drop(col('_partial_quantity_temp')) \
        .drop(col('mdl_bld_dt')) \
        .repartition(60)

    print("temp_df_flat")
    # _temp_df_flat.filter(col('customernumber').like('0500064458') & col('mat_no').like('000000000000119826')).show()

    _remainder_df = _get_remainder(sqlContext=sqlContext, testing=TESTING).repartition(60)

    print ("remainder_df")
    # _remainder_df.filter(col('customernumber').like('0500064458') & col('mat_no').like('000000000000119826')).show()

    print("result_df_stage")
    result_df_stage = _temp_df_flat \
        .select(col('customernumber'), col('mat_no'), col('delivery_date'), col('_partial_quantity')) \
        .withColumn('order_date', lit(order_date)) \
        .groupBy(col('customernumber'), col('mat_no'), col('order_date'), col('delivery_date')) \
        .agg(
        round(sum(col('_partial_quantity')), 2).alias('quantity_stg')
    ) \
        .repartition(60)

    # result_df_stage.filter(col('customernumber').like('0500064458') & col('mat_no').like('000000000000119826')).show()

    print("Writing raw invoice to HDFS")
    result_df_stage \
        .coalesce(1) \
        .write.mode('append') \
        .format('orc') \
        .option("header", "false") \
        .save(_PREDICTION_LOCATION)

    result_df_stage_remainder_df_cond = [result_df_stage.customernumber == _remainder_df.customernumber,
                                         result_df_stage.mat_no == _remainder_df.mat_no]

    result_df = result_df_stage \
        .join(_remainder_df, on=result_df_stage_remainder_df_cond, how='left_outer') \
        .select(result_df_stage.customernumber,
                result_df_stage.mat_no,
                result_df_stage.order_date,
                result_df_stage.delivery_date,
                result_df_stage.quantity_stg,
                _remainder_df.remainder.alias('carryover')
                ) \
        .na.fill(0.0) \
        .withColumn('quantity_temp', round((col('quantity_stg') + col('carryover')), 2).cast(FloatType())) \
        .withColumn('quantity', floor(col('quantity_temp')).cast(FloatType())) \
        .withColumn('remainder', round((col('quantity_temp') - col('quantity')), 2).cast(FloatType())) \
        .select(col('customernumber'), col('mat_no'), col('order_date'), col('delivery_date'), col('quantity'),
                col('remainder')) \
        .repartition(60)

    # .withColumn('carryover', when(col('carryover_stg') == None, 0.0).otherwise(col('carryover_stg')).cast(FloatType()))\

    # .drop(col('carryover_stg')) \

    print("result_df")
    # result_df.cache()
    # result_df.filter(col('customernumber').like('0500064458') & col('mat_no').like('000000000000119826')).show()
    # print(result_df.count())


    print("Writing invoice to HDFS")
    result_df \
        .coalesce(1) \
        .write.mode('append') \
        .format('orc') \
        .option("header", "false") \
        .save(FINAL_PREDICTION_LOCATION)



if __name__ == "__main__":

    order_dates = get_order_dates_between(start_date=START_DATE_ORDER, end_date=END_DATE_ORDER)
    print order_dates

    # for order_date in order_dates:
    #     print type(order_date)
    #     print get_criteria_date(order_date=order_date)
