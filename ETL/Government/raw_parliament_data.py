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

api_configs = spark.sql("SELECT * FROM default.api_configs where dataset_type = 'data' and dataset_sector = 'Parliament of India';")
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


