def _get_models_list(sqlContext, **kwargs):
    '''
    Returns all models < order_date for all cust-prod combo
    :param sqlContext: SQLContext
    :param kwargs: CRITERIA_DATE --> order date
    :return: Spark Dataframe of raw prediction data
    '''
    CRITERIA_DATE = kwargs.get('CRITERIA_DATE')
    testing = kwargs.get('testing')

    if testing:
        q = """select customernumber, mat_no, mdl_bld_dt 
        from predicted_order.view_consolidated_pred_complete_model_eda
        where to_date(from_unixtime(unix_timestamp(mdl_bld_dt , 'yyyy-MM-dd'))) < to_date(from_unixtime(unix_timestamp(""" + "\'" + CRITERIA_DATE + "\'" + """, 'yyyy-MM-dd')))
        """
    else:
        q = """select customernumber, mat_no, mdl_bld_dt 
        from predicted_order.final_table
        where to_date(from_unixtime(unix_timestamp(mdl_bld_dt , 'yyyy-MM-dd'))) < to_date(from_unixtime(unix_timestamp(""" + "\'" + CRITERIA_DATE + "\'" + """, 'yyyy-MM-dd')))
        """

    _model_bld_data = sqlContext.sql(q)

    return _model_bld_data


def _get_prediction_list(sqlContext, **kwargs):
    testing = kwargs.get('testing')

    if testing:
        q = """select customernumber, mat_no, mdl_bld_dt, pred_val, pdt_cat
        from predicted_order.view_consolidated_pred_complete_model_eda"""
    else:
        q = """select customernumber, mat_no, mdl_bld_dt, pred_val, pdt_cat
        from predicted_order.final_table"""

    _prediction_data = sqlContext.sql(q)
    return _prediction_data


if __name__ == "__main__":
    q = """select a.*, b.pred_val, b.pdt_cat, b.cutoff_date
        from
        (
        select customernumber, mat_no, MAX(mdl_bld_dt) latest_mdl_bld_dt 
        from predicted_order.final_table
        where mdl_bld_dt < """ + "\'" + '2017-09-14' + "\'" + """
        group by customernumber, mat_no
        ) a
        join
        (
        select customernumber, mat_no, pred_val, mdl_bld_dt, pdt_cat, cutoff_date from predicted_order.final_table
        ) b
        on a.customernumber = b.customernumber and a.mat_no = b.mat_no and a.latest_mdl_bld_dt = b.mdl_bld_dt"""

    print q