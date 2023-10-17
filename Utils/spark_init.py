# Databricks notebook source
import delta
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, ArrayType

# COMMAND ----------

# %run ./spark_init.ipynb

def get_spark() -> SparkSession:
    builder = (
        pyspark.sql.SparkSession.builder
        .master("spark://Subhashs-MacBook-Pro.local:7077")
        .appName("spark")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

if (SparkSession.getActiveSession()):
    print('Spark session is already active, using that..')
else:
    print('Starting a new spark session in cluster mode..')
    spark = get_spark()
