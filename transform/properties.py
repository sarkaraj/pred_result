CUSTOMER_LIST = ""

CUSTOMER_LIST_TABLE = """predicted_order.customerdata_CCBF"""

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
    from """ + CUSTOMER_LIST_TABLE \
            + """) sample_customer
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
