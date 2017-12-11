CUSTOMER_LIST = """('0500083147','0500061438','0500067084','0500058324','0500080723','0500060033','0500068825','0500060917','0500078551','0500076115','0500071747','0500078478','0500078038','0500073982','0500064458','0500268924','0500070702','0500070336','0500076032','0500095883','0500284889')"""

invoice_q = """
    select d.customernumber customernumber, d.matnr matnr, d.bill_date bill_date, IF(d.units != 'CS', d.quantity * (f.umrez / f.umren), d.quantity) quantity, d.dlvry_lag dlvry_lag
    from
    (
    select b.customernumber customernumber, b.matnr matnr, b.bill_date bill_date ,b.quantity quantity, b.units units, b.price price, c.dlvry_lag dlvry_lag
    from
    (
    select a.kunag customernumber, a.matnr matnr, a.fkdat bill_date ,a.fklmg quantity, a.meins units, a.netwr price
    from skuopt.invoices a
    ) b
    join
    (
    select sample_customer.customernumber customernumber, master_customer.dlvry_lag dlvry_lag
    from
    (
    select customernumber
    from predicted_order.view_sample_customer_FL_200
    ) sample_customer
    join
    (
    select kunnr customernumber, IF(vsbed == '01', 2, 1) dlvry_lag
    from mdm.customer
    ) master_customer
    on
    sample_customer.customernumber = master_customer.customernumber
    ) c
    on
    b.customernumber = c.customernumber
    ) d
    join
    (
    select e.matnr matnr, e.meinh meinh, e.umren umren, e.umrez umrez
    from mdm.dim_marm e
    ) f
    on
    d.matnr=f.matnr and d.units=f.meinh
    where"""

# VISIT_LIST_LOCATION = "wasb://skuopt@conapocv2standardsa.blob.core.windows.net/AZ_TCAS_VL.csv"
VISIT_LIST_LOCATION = "wasb://skuopt@conapocv2standardsa.blob.core.windows.net/cona.dsouth_Results_811_20171009142501.csv"
AGLU_LIST_LOCATION = "wasb://skuopt@conapocv2standardsa.blob.core.windows.net/AZ_TCAS_AGLU.csv"

if __name__ == "__main__":
    print invoice_q
