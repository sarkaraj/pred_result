from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
# from pyspark.sql.types import *
from pyspark.sql.functions import *
from transform._visit_list import _get_visit_list
from transform._aglu_list import _get_aglu_list

conf = SparkConf().setAppName("CSO_TESTING_")
# .setMaster("yarn-client")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sparkContext=sc)

print "Setting LOG LEVEL as ERROR"
sc.setLogLevel("ERROR")

print "Add jobs.zip to system path"
import sys

sys.path.insert(0, "cso.zip")

vl_df = _get_visit_list(sc=sc, sqlContext=sqlContext)
aglu_df = _get_aglu_list(sc=sc, sqlContext=sqlContext)

vl_df.select('KUNNR', 'del_date').registerTempTable("vl_sql")
aglu_df.registerTempTable("aglu_sql")

q = """
select e.*, f.matnr matnr
from
(
select b.kunnr customernumber, b.del_date del_date, d.scl_auth_matlst scl_auth_matlst, d.vkbur vkbur
from
(
select a.KUNNR kunnr, a.del_date del_date
from vl_sql a
where a.KUNNR in ('0500066337','0500070166','0500070167','0500075749','0500083147','0500061438','0500067084','0500058324','0500080723','0500060033','0500068825','0500060917','0500078551','0500076115','0500071747','0500078478','0500078038','0500073982','0500064458','0500268924','0500070702','0500070336','0500076032','0500095883','0500284889')
) b
join
(
select c.kunnr kunnr, c.scl_auth_matlst scl_auth_matlst, c.vkbur vkbur
from mdm.customer c
) d
on d.kunnr = b.kunnr
) e
cross join
(
select g.MATNR matnr, g.SCL_AUTH_MATLST scl_auth_matlst
from aglu_sql g
) f
where e.scl_auth_matlst = f.scl_auth_matlst
"""

visit_list_final = sqlContext.sql(q)

visit_list_final.show()

# print visit_list_final.count()
