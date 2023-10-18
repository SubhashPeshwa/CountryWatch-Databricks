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
from ETL.Common.api import call_data_gov_api

# COMMAND ----------

# MAGIC %run ../../Utils/spark_init

# COMMAND ----------

dbutils.widgets.dropdown("load_type", "full_load", ["full_load", "sector_load"])
dbutils.widgets.text("dataset_sector", "Parliament of India")
load_type = dbutils.widgets.get("load_type")
dataset_sector = dbutils.widgets.get("dataset_sector")

# COMMAND ----------

if load_type == "full_load":
    api_configs = spark.sql("SELECT * FROM default.api_configs where dataset_type = 'data';")
else:
    api_configs = spark.sql("SELECT * FROM default.api_configs where dataset_type = 'data' and dataset_sector = '{}';".format(dataset_sector))
display(api_configs)

# COMMAND ----------

for api_config in api_configs.collect():

    table_schema = StructType.fromJson(json.loads(api_config['table_schema'].replace('\\"','"')))

    resp = call_data_gov_api(api_config['dataset_uri'])

    df = spark.createDataFrame(data=resp.json()['records'], schema = table_schema)
    df.write.mode("overwrite").saveAsTable(api_config['table_name'])

    display(spark.sql('DESCRIBE DETAIL default.{};'.format(api_config['table_name'])))
    # display(spark.sql("SELECT * FROM default.{};".format(api_config['table_name'])))

# COMMAND ----------


