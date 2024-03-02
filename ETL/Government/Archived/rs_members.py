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

dir = os.getcwd()
uri = "/resource/fa52dc7f-7292-4ece-8f07-900322b2c9f9"
data_gov_url = "https://data.gov.in/resource/list-rajya-sabha-members-english"
data_gov_name = "List of Rajya Sabha Members (English)"
table_name = "rs_members"

resp = call_data_gov_api(uri)
# print(resp.json()['records'])

json_array_schema = StructType([
    StructField('sno', StringType(), nullable=False), 
    StructField('mpcode', StringType(), nullable=False),
    StructField('membername', StringType(), nullable=False),
    StructField('gender', StringType(), nullable=False),
    StructField('party', StringType(), nullable=False),
    StructField('partyname', StringType(), nullable=False),
    StructField('state', StringType(), nullable=False), 
    StructField('statename', StringType(), nullable=False),
    StructField('permanentaddress', StringType(), nullable=False),
    StructField('permanentphone', StringType(), nullable=False),
    StructField('localaddress', StringType(), nullable=False),
    StructField('localphone', StringType(), nullable=False),
    StructField('emailid', StringType(), nullable=False)
  ])

# Read as a dictionary
df = spark.createDataFrame(data=resp.json()['records'], schema = json_array_schema)

df.write.mode("overwrite").saveAsTable(table_name)

display(spark.sql('DESCRIBE DETAIL default.{};'.format(table_name)))
display(spark.sql("SELECT * FROM default.{};".format(table_name)).show())

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


