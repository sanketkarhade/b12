'''
SCRIPT NAME: L1_ref_data_load.py
SCRIPT VER: 1.0
DESCRIPTION: this will load ref data in l1 layer
CREATED BY: Sayu Softtech Pvt Ltd
CREATED DATE: 23-05-2022
MODIFIED DATE:
'''

from pyspark.sql import SparkSession
import datetime

spark = SparkSession.builder.appName("L1 Ref Data Load").master("yarn").enableHiveSupport().config("spark.sql.parquet.writeLegacyFormat","true").getOrCreate()

s3_input_path="s3://bigdatatraining123/input_hist"
tgt_schema="prod"

def read_data(spark,s3_input_path,table):
	input_path="{0}/{1}.snappy.parquet".format(s3_input_path,table)
	#input_path="s3://bigdatatraining123/input_hist/COUNTRY.snappy.parquet"
	df=spark.read.parquet(input_path)
	df.show(5)
	print("Data Read completed for {0}".format(table))
	return df

def write_data(spark,tgt_schema,s3_input_path,app_name,src_table,tgt_table):
	src_data=read_data(spark,s3_input_path,src_table)
	write_path="/user/hive/warehouse/{0}.db/{1}".format(tgt_schema,tgt_table)
	src_data.write.mode('overwrite').parquet(write_path)
	print("Data Successfully written for {0}.{1}".format(tgt_schema,tgt_table))
	count=src_data.count()
	audit_entry(tgt_schema,app_name,tgt_table,count)
	
#Audit entry
def audit_entry(tgt_schema,app_name,tgt_table,count):
	curr_time=datetime.datetime.now()
	query="insert into {0}.audit_data values('{1}','{2}','{3}',{4})".format(tgt_schema,curr_time,app_name,tgt_table,count)
	spark.sql(query)
	print("Audit Entry Captured")

print("Job started...")

#data write
write_data(spark,tgt_schema,s3_input_path,"L1_DATA_LOAD","COUNTRY","country_l1")
write_data(spark,tgt_schema,s3_input_path,"L1_DATA_LOAD","CITY","city_l1")
write_data(spark,tgt_schema,s3_input_path,"L1_DATA_LOAD","TX_TYPE","tx_type_l1")
write_data(spark,tgt_schema,s3_input_path,"L1_DATA_LOAD","CARD_TYPE","card_type_l1")

spark.stop()

print("Job Finished SuccessFully!!!")

#spark-submit --master yarn --deploy-mode client --executor-memory 3G /home/hadoop/03.L1_ref_data_load.py

