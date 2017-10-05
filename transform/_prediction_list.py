def _get_prediction_list(sc, sqlContext):
    q = """
    select customernumber, mat_no matnr, IF(pred_val < 0, 0, pred_val) pred_val, pdt_cat from predicted_order.cso_prediction
    """

    _prediction_data = sqlContext.sql(q)

    return _prediction_data
