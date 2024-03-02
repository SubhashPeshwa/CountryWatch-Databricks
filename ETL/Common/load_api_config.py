# Databricks notebook source
# To Do: Add SCD to this with date fields

# COMMAND ----------

import os
import json
import requests
import delta
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, ArrayType, MapType

# COMMAND ----------

# List all JSON files in the folder
# data_json_files = [os.path.join("./api_configs/source_data", file) for file in os.listdir("./api_configs/source_data") if file.endswith(".json")]
# catalog_json_files = [os.path.join("./api_configs/source_catalog", file) for file in os.listdir("./api_configs/source_catalog") if file.endswith(".json")]
# json_files = data_json_files
# json_files.extend(catalog_json_files)

json_files = []

for dirpath, dirnames, filenames in os.walk("./api_configs"):
    for file in filenames:
        if file.endswith(".json"):
            json_files.append(os.path.join(dirpath, file))

configs = []

table_name = "api_configs"
config_schema = StructType([
    StructField("dataset_name", StringType(), nullable=False),
    StructField("dataset_uri", StringType(), nullable=False),
    StructField("dataset_public_url", StringType(), nullable=False),
    StructField("data_source", StringType(), nullable=False),
    StructField("dataset_type", StringType(), nullable=False),
    StructField("dataset_sector", StringType(), nullable=False),
    StructField("table_name", StringType(), nullable=False),
    StructField("table_schema", StringType(), nullable=False)
])

for json_file in json_files:
    with open(json_file, "r") as file:
        json_data = json.load(file)
        # Convert the "table_schema" to a JSON string with escaped double quotes
        escaped_table_schema = json.dumps(json_data["table_schema"]).replace('"', '\\"')

        # Create a new JSON object with the escaped "table_schema"
        escaped_data = json_data.copy()
        escaped_data["table_schema"] = escaped_table_schema

        configs.append(escaped_data)

df = spark.createDataFrame(data=configs, schema = config_schema)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

# COMMAND ----------

display(spark.sql('DESCRIBE DETAIL default.{};'.format(table_name)))
display(spark.sql("SELECT * FROM default.{};".format(table_name)))

# COMMAND ----------

spark.sql("DROP TABLE api_configs")

# COMMAND ----------

tables_df = spark.sql("show tables")
for table in tables_df.collect():
    print(f"Dropping table {table['tableName']}")
    spark.sql(f"DROP TABLE {table['tableName']}")
display(spark.sql("show tables"))

# COMMAND ----------


