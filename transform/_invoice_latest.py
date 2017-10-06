def _get_invoice_data(sqlContext, **kwargs):
    CRITERIA_DATE = kwargs.get('CRITERIA_DATE')
    q_2 = """
    select a.kunag customernumber, a.matnr matnr, max(a.fkdat) bill_date
    from skuopt.invoices a
    where a.kunag in ('0500066337','0500070166','0500070167','0500075749','0500083147','0500061438','0500067084','0500058324','0500080723','0500060033','0500068825','0500060917','0500078551','0500076115','0500071747','0500078478','0500078038','0500073982','0500064458','0500268924','0500070702','0500070336','0500076032','0500095883','0500284889')
    and a.fkdat < """ + CRITERIA_DATE + """ group by a.kunag,a.matnr"""

    _invoice_raw_data = sqlContext.sql(q_2)

    return _invoice_raw_data


if __name__ == "__main__":
    q_2 = """
        select a.kunag customernumber, a.matnr matnr, max(a.fkdat) bill_date
        from skuopt.invoices a
        where a.kunag in ('0500066337','0500070166','0500070167','0500075749','0500083147','0500061438','0500067084','0500058324','0500080723','0500060033','0500068825','0500060917','0500078551','0500076115','0500071747','0500078478','0500078038','0500073982','0500064458','0500268924','0500070702','0500070336','0500076032','0500095883','0500284889')
        and a.fkdat < """ + CRITERIA_DATE + """ group by a.kunag,a.matnr"""

    print q_2
