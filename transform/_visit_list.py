from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import datetime as dt
from properties import VISIT_LIST_LOCATION


def custom_parser (input):
	from datetime import datetime
	# input = input.replace('"', '')

	datetime_object = datetime.strptime(input, '\"%m/%d/%Y %I:%M:%S %p\"').strftime('%Y-%m-%d')

	return datetime_object


# my_parser = udf(custom_parser, StringType())


def _schema_vl ():
	MANDT = StructField("MANDT", StringType(), nullable = True)
	VPTYP = StructField("VPTYP", StringType(), nullable = True)
	EXDAT = StructField("EXDAT", StringType(), nullable = True)
	KUNNR = StructField("KUNNR", StringType(), nullable = True)
	FROMTIME = StructField("FROMTIME", StringType(), nullable = True)
	TOTIME = StructField("TOTIME", StringType(), nullable = True)
	USERID = StructField("USERID", StringType(), nullable = True)
	VKORG = StructField("VKORG", StringType(), nullable = True)
	VTWEG = StructField("VTWEG", StringType(), nullable = True)
	SPARTE = StructField("SPARTE", StringType(), nullable = True)
	AUTH = StructField("AUTH", StringType(), nullable = True)
	VLID = StructField("VLID", StringType(), nullable = True)
	VLPOS = StructField("VLPOS", StringType(), nullable = True)
	SEQU = StructField("SEQU", StringType(), nullable = True)
	VPID = StructField("VPID", StringType(), nullable = True)
	DATE_SENT = StructField("DATE_SENT", StringType(), nullable = True)
	TIME_SENT = StructField("TIME_SENT", StringType(), nullable = True)
	DELETE_FLAG = StructField("DELETE_FLAG", StringType(), nullable = True)
	VCTEXT = StructField("VCTEXT", StringType(), nullable = True)
	STIME = StructField("STIME", StringType(), nullable = True)
	STIMEZONE = StructField("STIMEZONE", StringType(), nullable = True)

	schema = StructType(
		[MANDT, VPTYP, EXDAT, KUNNR, FROMTIME, TOTIME, USERID, VKORG, VTWEG, SPARTE, AUTH, VLID, VLPOS, SEQU, VPID,
		 DATE_SENT, TIME_SENT, DELETE_FLAG, VCTEXT, STIME, STIMEZONE])
	return schema


def _schema_aglu ():
	MANDT = StructField("MANDT", StringType(), nullable = True)
	VKORG = StructField("VKORG", StringType(), nullable = True)
	VTWEG = StructField("VTWEG", StringType(), nullable = True)
	AUTH_MATLST = StructField("AUTH", StringType(), nullable = True)
	MATNR = StructField("MATNR", StringType(), nullable = True)
	LOEVM = StructField("LOEVM", StringType(), nullable = True)
	AENDT = StructField("AENDT", StringType(), nullable = True)
	AENUHR = StructField("AENUHR", StringType(), nullable = True)

	schema = StructType([MANDT, VKORG, VTWEG, AUTH_MATLST, MATNR, LOEVM, AENDT, AENUHR])
	return schema


def _get_visit_list (sc, sqlContext, order_date, **kwargs):
	if 'visit_list' in kwargs.keys():
		vl = kwargs.get('visit_list')
	else:
		raw_data = sc.textFile(VISIT_LIST_LOCATION)
		header_vl = raw_data.first()

		vl_temp = raw_data \
			.filter(lambda x: x != header_vl) \
			.map(lambda line: line.split(","))

		vl = sqlContext.createDataFrame(vl_temp, schema = _schema_vl()) \
			.withColumn('order_date',
						from_unixtime(unix_timestamp(col('DATE_SENT'), "yyyy.MM.dd")).cast(DateType()).alias(
							'order_date')) \
			.select(col('KUNNR').alias('customernumber'), col('order_date'))

	vl_df = vl \
		.filter(col('order_date') == order_date)

	return vl_df


# # TODO: NOT USED TILL NOW -- MOST LIKELY TO BE DELETED
def _get_generated_visit_list (sc, sqlContext):
	invoice_q = """
    select d.customernumber customernumber, d.matnr matnr, d.bill_date bill_date, IF(d.units != 'CS', d.quantity * (
    f.umrez / f.umren), d.quantity) quantity, d.dlvry_lag dlvry_lag
    from
    (
    select b.customernumber customernumber, b.matnr matnr, b.bill_date bill_date ,b.quantity quantity, b.units units, 
    b.price price, c.dlvry_lag dlvry_lag
    from
    (
    select a.kunag customernumber, a.matnr matnr, a.fkdat bill_date ,a.fklmg quantity, a.meins units, a.netwr price
    from skuopt.invoices a
    where a.kunag in ('0500083147','0500061438','0500067084','0500058324','0500080723','0500060033','0500068825',
    '0500060917','0500078551','0500076115','0500071747','0500078478','0500078038','0500073982','0500064458',
    '0500268924','0500070702','0500070336','0500076032','0500095883','0500284889')
    ) b
    join
    (
    select kunnr customernumber, IF(vsbed == '01', 2, 1) dlvry_lag
    from mdm.customer
    where vkbur='C005'
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
    where d.bill_date >= '20170903' and d.bill_date <= '20171007'
    """

	invoice_raw = sqlContext.sql(invoice_q)
	invoice_raw.cache()

	data_set = invoice_raw \
		.filter(col('quantity') > 0) \
		.withColumn('b_date', from_unixtime(unix_timestamp(col('bill_date'), "yyyyMMdd")).cast(DateType())) \
		.withColumn('dlvry_date', udf((lambda x: x.strftime('%Y-%m-%d')), StringType())(col('b_date'))) \
		.withColumn('bus_day_flag',
					udf((lambda x: str(np.is_busday([x])[0])), StringType())(col('dlvry_date')).cast(BooleanType())) \
		.withColumn('visit_date',
					udf((lambda x, y: str(np.busday_offset(x, -y, roll = 'backward'))), StringType())(col(
						'dlvry_date'),
						col(
							'dlvry_lag'))) \
		.select(col('customernumber'), col('visit_date')) \
		.distinct()
	return None


def get_visit_list_w_mat_list (sc, sqlContext, **kwargs):
	param_set = {"_repartitions": p_.REPARTITION_FACTOR,
				 "vl_location": p_.VISIT_LIST_LOCATION,
				 "aglu_location": p_.AGLU_LIST_LOCATION,
				 "vl_schema": _schema_vl(),
				 "aglu_schema": _schema_aglu(),
				 "date_str_parse_format": "yyyy.MM.dd"
				 }

	for key in param_set.keys():
		if key in kwargs.keys():
			param_set[key] = kwargs[key]
		else:
			pass

	_repartitions = param_set.get("_repartitions")
	VISIT_LIST_LOCATION = param_set.get("vl_location")
	AGLU_LIST_LOCATION = param_set.get("aglu_location")
	vl_schema = param_set.get("vl_schema")
	aglu_schema = param_set.get("aglu_schema")
	date_str_parse_format = param_set.get("date_str_parse_format")

	raw_vl_head = sc.textFile(VISIT_LIST_LOCATION).first()
	raw_vl = sc.textFile(VISIT_LIST_LOCATION) \
		.filter(lambda x: x != raw_vl_head) \
		.map(lambda x: x.split(",")) \
		.map(lambda x: [None if elem == "" else elem for elem in x])

	# print(raw_vl.take(1))
	# print(raw_vl_head)

	raw_aglu_head = sc.textFile(AGLU_LIST_LOCATION).first()
	raw_aglu = sc.textFile(AGLU_LIST_LOCATION) \
		.filter(lambda x: x != raw_aglu_head) \
		.map(lambda x: x.split(",")) \
		.map(lambda x: [None if elem == "" else elem for elem in x])

	# print(raw_aglu.take(1))
	# print(raw_aglu_head)

	vl_raw_df = sqlContext.createDataFrame(raw_vl, schema = vl_schema) \
		.repartition(_repartitions) \
		.filter(col("VPTYP") == "ZR") \
		.withColumn("EXDAT_F", from_unixtime(unix_timestamp(col("EXDAT"), date_str_parse_format)).cast(DateType())) \
		.drop(col("EXDAT")) \
		.withColumnRenamed("EXDAT_F", "EXDAT")

	# TODO: Add one more filter to filter out the dates as per the order_date

	# vl_raw_df.take(1)
	# vl_raw_df.cache()

	aglu_raw_df = sqlContext.createDataFrame(raw_aglu, schema = aglu_schema) \
		.coalesce(1)

	# aglu_raw_df.take(1)
	# aglu_raw_df.cache()

	# print("Visit List")
	# vl_raw_df.show(10)

	# print("Authorization List")
	# aglu_raw_df.show(10)

	visit_list = vl_raw_df \
		.select(rtrim(ltrim(col("KUNNR"))).alias("KUNNR"),
				rtrim(ltrim(col("VKORG"))).alias("VKORG"),
				rtrim(ltrim(col("AUTH"))).alias("AUTH"),
				rtrim(ltrim(col("VTWEG"))).alias("VTWEG"),
				rtrim(ltrim(col("MANDT"))).alias("MANDT"),
				col("EXDAT").alias("ORDER_DATE")
				)

	# print("Count of visit list")
	# print(visit_list.count())

	auth_list = aglu_raw_df \
		.select(rtrim(ltrim(col("VKORG"))).alias("VKORG"),
				rtrim(ltrim(col("AUTH"))).alias("AUTH"),
				rtrim(ltrim(col("MANDT"))).alias("MANDT"),
				rtrim(ltrim(col("VTWEG"))).alias("VTWEG"),
				rtrim(ltrim(col("MATNR"))).alias("MATNR")
				) \
		.dropDuplicates(["MATNR"]) \
		.na.drop(how = 'any', subset = ["MATNR"])

	# print("Count of authorisation list")
	# print(auth_list.count())
	# auth_list.show(5)

	visit_auth_condition = [visit_list.VKORG == auth_list.VKORG,
							visit_list.MANDT == auth_list.MANDT,
							visit_list.VTWEG == auth_list.VTWEG,
							visit_list.AUTH == auth_list.AUTH]

	visit_list_join_auth_list = visit_list \
		.join(broadcast(auth_list),
			  on = visit_auth_condition,
			  how = "left") \
		.drop(auth_list.VKORG) \
		.drop(auth_list.AUTH) \
		.drop(auth_list.MANDT) \
		.drop(auth_list.VTWEG) \
		.na.drop(how = 'any', subset = ["MATNR"])

	return visit_list_join_auth_list


def blank_as_null (x):
	return when(col(x) != "", col(x)).otherwise(None)


if __name__ == "__main__":
	import numpy as np
	from pyspark import SparkContext, SparkConf
	from pyspark.sql import HiveContext, SQLContext
	import properties as p_

	# print np.busday_offset('2017-09-15', 2, roll='forward')
	# print str(np.busday_offset('2017-09-15', 2, roll='forward'))

	conf = SparkConf() \
		.setAppName("testing") \
		.setMaster("local[*]") \
		.set("spark.io.compression.codec", "snappy") \
		.set("spark.driver.extraClassPath",
			 "C:\Users\Rajarshi Sarkar\PycharmProjects\pred_result\external_jars\spark-csv_2.10-1.5.0.jar") \
		.set("spark.executor.extraClassPath",
			 "C:\Users\Rajarshi Sarkar\PycharmProjects\pred_result\external_jars\spark-csv_2.10-1.5.0.jar")

	sc = SparkContext(conf = conf)
	# sqlContext = HiveContext(sparkContext = sc)

	sqlContext = SQLContext(sparkContext = sc)

	print "Setting LOG LEVEL as ERROR"
	sc.setLogLevel("ERROR")

	_repartitions = p_.REPARTITION_FACTOR

	df = get_visit_list_w_mat_list(sc = sc, sqlContext = sqlContext,
								   vl_location = "C:\\Users\\Rajarshi Sarkar\\Desktop\\visit_list\\AZ_TCAS_VL.csv",
								   aglu_location = "C:\\Users\\Rajarshi Sarkar\\Desktop\\visit_list\\AZ_TCAS_AGLU.csv")

	df.show(10)
# print(df.count())
#
# raw_vl_head = sc.textFile("C:\\Users\\Rajarshi Sarkar\\Desktop\\visit_list\\AZ_TCAS_VL.csv").first()
# raw_vl = sc.textFile("C:\\Users\\Rajarshi Sarkar\\Desktop\\visit_list\\AZ_TCAS_VL.csv") \
# 	.filter(lambda x: x != raw_vl_head) \
# 	.map(lambda x: x.split(",")) \
# 	.map(lambda x: [None if elem == "" else elem for elem in x])
#
# # print(raw_vl.take(1))
# # print(raw_vl_head)
#
# raw_aglu_head = sc.textFile("C:\\Users\\Rajarshi Sarkar\\Desktop\\visit_list\\AZ_TCAS_AGLU.csv").first()
# raw_aglu = sc.textFile("C:\\Users\\Rajarshi Sarkar\\Desktop\\visit_list\\AZ_TCAS_AGLU.csv") \
# 	.filter(lambda x: x != raw_aglu_head) \
# 	.map(lambda x: x.split(",")) \
# 	.map(lambda x: [None if elem == "" else elem for elem in x])
#
# # print(raw_aglu.take(1))
# # print(raw_aglu_head)
#
# vl_schema = _schema_vl()
# aglu_schema = _schema_aglu()
#
# vl_raw_df = sqlContext.createDataFrame(raw_vl, schema = vl_schema) \
# 	.repartition(_repartitions) \
# 	.filter(col("VPTYP") == "ZR") \
# 	.withColumn("EXDAT_F", from_unixtime(unix_timestamp(col("EXDAT"), "yyyy.MM.dd")).cast(DateType())) \
# 	.drop(col("EXDAT")) \
# 	.withColumnRenamed("EXDAT_F", "EXDAT")
#
# # TODO: Add one more filter to filter out the dates as per the order_date
#
# # vl_raw_df.take(1)
# # vl_raw_df.cache()
#
# aglu_raw_df = sqlContext.createDataFrame(raw_aglu, schema = aglu_schema) \
# 	.coalesce(1)
#
# # aglu_raw_df.take(1)
# # aglu_raw_df.cache()
#
# # print("Visit List")
# # vl_raw_df.show(10)
#
# # print("Authorization List")
# # aglu_raw_df.show(10)
#
# visit_list = vl_raw_df \
# 	.select(rtrim(ltrim(col("KUNNR"))).alias("KUNNR"),
# 			rtrim(ltrim(col("VKORG"))).alias("VKORG"),
# 			rtrim(ltrim(col("AUTH"))).alias("AUTH"),
# 			rtrim(ltrim(col("VTWEG"))).alias("VTWEG"),
# 			rtrim(ltrim(col("MANDT"))).alias("MANDT"),
# 			col("EXDAT").alias("ORDER_DATE")
# 			)
#
# # print("Count of visit list")
# # print(visit_list.count())
#
# auth_list = aglu_raw_df \
# 	.select(rtrim(ltrim(col("VKORG"))).alias("VKORG"),
# 			rtrim(ltrim(col("AUTH"))).alias("AUTH"),
# 			rtrim(ltrim(col("MANDT"))).alias("MANDT"),
# 			rtrim(ltrim(col("VTWEG"))).alias("VTWEG"),
# 			rtrim(ltrim(col("MATNR"))).alias("MATNR")
# 			) \
# 	.dropDuplicates(["MATNR"]) \
# 	.dropna(how = 'any', subset = ["MATNR"])
#
# # print("Count of authorisation list")
# # print(auth_list.count())
# # auth_list.show(5)
#
# visit_auth_condition = [visit_list.VKORG == auth_list.VKORG,
# 						visit_list.MANDT == auth_list.MANDT,
# 						visit_list.VTWEG == auth_list.VTWEG,
# 						visit_list.AUTH == auth_list.AUTH]
#
#
# visit_list_join_auth_list = visit_list \
# 	.join(broadcast(auth_list),
# 		  on = visit_auth_condition,
# 		  how = "left") \
# 	.drop(auth_list.VKORG) \
# 	.drop(auth_list.AUTH) \
# 	.drop(auth_list.MANDT) \
# 	.drop(auth_list.VTWEG)
#
#
# print("Count of joint dataset")
# visit_list_join_auth_list.show(10)
#
# print(visit_list_join_auth_list.count())

# 	print("Dataset Verifications --- TEMPORARY\n\n")
# 	print("VISIT LIST DATASET CHECK...\n")
#
# 	auth_list\
# 		.groupBy(["MANDT", "VKORG", "VTWEG", "AUTH"])\
# 		.agg(count(col("MATNR")).alias("count"))\
# 		.orderBy(col("count").desc())\
# 		.show()
# #
#
# 	auth_list\
# 		.groupBy(["MANDT", "VKORG", "VTWEG", "AUTH"])\
# 		.agg(countDistinct(col("MATNR")).alias("count"))\
# 		.orderBy(col("count").desc())\
# 		.show()
#
# 	visit_list_join_auth_list\
# 		.groupBy(col("KUNNR"), col("ORDER_DATE"))\
# 		.agg(count(col("MATNR")).alias("pdt_count"))\
# 		.orderBy(col("pdt_count").desc())\
# 		.show()
#
#
# 	visit_list_join_auth_list\
# 		.filter(col("KUNNR")=='0600426408')\
# 		.groupBy(col("KUNNR"), col("ORDER_DATE"))\
# 		.agg(count(col("MATNR")).alias("pdt_count"))\
# 		.orderBy(col("pdt_count").desc())\
# 		.show()
#
# 	visit_list_join_auth_list\
# 		.groupBy(col("KUNNR"), col("ORDER_DATE"))\
# 		.agg(count(col("MATNR")).alias("pdt_count"))\
# 		.orderBy(col("pdt_count"))\
# 		.show()
#
#
# 	visit_list_join_auth_list\
# 		.filter(col("KUNNR")=='0601012094')\
# 		.groupBy(col("KUNNR"), col("ORDER_DATE"))\
# 		.agg(count(col("MATNR")).alias("pdt_count"))\
# 		.orderBy(col("pdt_count"))\
# 		.show()
