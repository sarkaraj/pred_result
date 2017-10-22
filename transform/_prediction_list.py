def _get_prediction_list(sc, sqlContext, **kwargs):
    CRITERIA_DATE = kwargs.get('CRITERIA_DATE')

    q = """select a.*, b.pred_val, b.pdt_cat, b.cutoff_date
    from
    (
    select customernumber, mat_no, MAX(mdl_bld_dt) latest_mdl_bld_dt 
    from predicted_order.final_table
    where mdl_bld_dt < """ + "\'" + CRITERIA_DATE + "\'" + """
    group by customernumber, mat_no
    ) a
    join
    (
    select customernumber, mat_no, pred_val, mdl_bld_dt, pdt_cat, cutoff_date from predicted_order.final_table
    ) b
    on a.customernumber = b.customernumber and a.mat_no = b.mat_no and a.latest_mdl_bld_dt = b.mdl_bld_dt
    limit 10"""

    _prediction_data = sqlContext.sql(q)

    return _prediction_data


if __name__ == "__main__":
    q = """select a.*, b.pred_val, b.pdt_cat, b.cutoff_date
        from
        (
        select customernumber, mat_no, MAX(mdl_bld_dt) latest_mdl_bld_dt 
        from predicted_order.final_table
        where mdl_bld_dt < """ + "\'" + '2017-09-15' + "\'" + """
        group by customernumber, mat_no
        ) a
        join
        (
        select customernumber, mat_no, pred_val, mdl_bld_dt, pdt_cat, cutoff_date from predicted_order.final_table
        ) b
        on a.customernumber = b.customernumber and a.mat_no = b.mat_no and a.latest_mdl_bld_dt = b.mdl_bld_dt
        limit 10"""

    print q