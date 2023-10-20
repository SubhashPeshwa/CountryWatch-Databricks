# Databricks notebook source
import os
import json
import requests
import delta
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, ArrayType

import sys
sys.path.append('../..')
from ETL.Common.api import call_data_gov_catalog

# COMMAND ----------

# MAGIC %run ../../Utils/spark_init

# COMMAND ----------

api_configs = spark.sql("SELECT * FROM default.api_configs where dataset_type = 'source catalog' and dataset_sector = 'Data.gov Catalog';")
display(api_configs)

# COMMAND ----------

for api_config in api_configs.collect():
    if api_config['table_name'] == "data_gov_catalog":
        continue
    table_schema = StructType.fromJson(json.loads(api_config['table_schema'].replace('\\"','"')))

    # field_sector = "Lok%20Sabha"
    field_sector = ""
    resp = call_data_gov_catalog(api_config['dataset_uri'], field_sector)
    print(resp)
    print(resp.json())
    df = spark.createDataFrame(data=resp.json()['data']['rows'], schema = table_schema)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(api_config['table_name'])

    display(spark.sql('DESCRIBE DETAIL default.{};'.format(api_config['table_name'])))
    display(spark.sql("SELECT * FROM default.{};".format(api_config['table_name'])))

# COMMAND ----------

dftbl = sqlContext.sql("show tables")
dfdbs = sqlContext.sql("show databases")
for row in dfdbs.rdd.collect():
    tmp = "show tables from " + row['databaseName'] 
    if row['databaseName'] == 'default':
        dftbls = sqlContext.sql(tmp)
    else:
       dftbls = dftbls.union(sqlContext.sql(tmp))
tmplist = []
for row in dftbls.rdd.collect():
    try:
      tmp = 'select count(*) myrowcnt from ' + row['database'] + '.' + row['tableName']
      tmpdf = sqlContext.sql(tmp)
      myrowcnt= tmpdf.collect()[0]['myrowcnt'] 
      tmplist.append((row['database'], row['tableName'],myrowcnt))
    except:
      tmplist.append((row['database'], row['tableName'],-1))

columns =  ['database', 'tableName', 'rowCount']     
df = spark.createDataFrame(tmplist, columns)    
display(df)

# COMMAND ----------


