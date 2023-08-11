# Databricks notebook source
import json
import requests
import os
import logging
from datetime import datetime, timedelta
from base64 import b64encode
from delta.tables import *
from pyspark.sql.types import *
from typing import List
from pyspark.sql.functions import col, asc
from pyspark.sql import *

# COMMAND ----------

# MAGIC %run "../Utils/Azure Utils"

# COMMAND ----------

def call_data_gov_api(uri):
    """
    """

    url = "https://api.data.gov.in"
    api_key = get_secret("kvdevaue-secret-scope","DATA-GOV-API-KEY")
    resp_format = "json"
    limit = 500
    offset = 0
    resp = requests.get("{}{}?api-key={}&format={}&limit={}&offset={}".format(url,uri,api_key,resp_format,limit,offset))

    return resp

# COMMAND ----------

uri = "/resource/81153f15-b4da-45b5-a299-f307351c5001"
resp = call_data_gov_api(uri)

# COMMAND ----------

resp.json()['records']

# COMMAND ----------

df = spark.read.json(resp.json()['records'])

# COMMAND ----------

from pyspark.sql.functions import col, udf
from pyspark.sql.types import *
import json
# Schema for the array of JSON objects.
json_array_schema = ArrayType(
  StructType([
    StructField('Sub1', StringType(), nullable=False), 
    StructField('Sub2', IntegerType(), nullable=False)
  ])
)
# Create function to parse JSON using standard Python json library.
def parse_json(array_str):
  json_obj = json.loads(array_str)
  for item in json_obj:
    yield (item['Sub1'], item['Sub2'])
# Create a UDF, whose return type is the JSON schema defined above.    
parse_json_udf = udf(lambda str: parse_json(str), json_array_schema)
  
# Use the UDF to change the JSON string into a true array of structs.
test3DF = test3DF.withColumn("JSON1arr", parse_json_udf((col("JSON1"))))
# We don't need to JSON text anymore.
test3DF = test3DF.drop("JSON1")



from pyspark.sql.functions import col, explode
test3DF = test3DF.withColumn("JSON1obj", explode(col("JSON1arr")))
# The column with the array is now redundant.
test3DF = test3DF.drop("JSON1arr")



