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

# MAGIC %md
# MAGIC ## Loggers

# COMMAND ----------

#Functions
class utils:
  @staticmethod
  def initLogger(notebookName):
    '''Intiatilise logging object for a specific notebook'''
    global logger
    # initialize logger when function is called
    logger = logging.getLogger(notebookName)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', "%Y-%m-%d %H:%M:%S")
    ch.setFormatter(formatter)
    logging.getLogger().handlers = []
    logger.handlers = []
    logger.addHandler(ch)
    logger.debug("logger initialized")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Azure Operations

# COMMAND ----------

# Function to get secret value from Key-vault backed secret scopes
# Create secret scope by using cli or by going here - https://adb-3064869328147302.2.azuredatabricks.net/?o=3064869328147302#secrets/createScope
def get_secret(scope_name, secret_name):      
  return dbutils.secrets.get(scope = scope_name, key = secret_name)

# COMMAND ----------

def mount_storage(mount_path, datalake_name, datalake_key, source_mount):
 
  if any(mount.mountPoint == mount_path for mount in dbutils.fs.mounts()):
      print("{} -- Mount Exists & will not be mounted".format(mount_path))
  else:
      print("{} -- Will be mounted".format(mount_path))
      print("Mounting...")
      dlKey = "fs.azure.account.key.{}.blob.core.windows.net".format(datalake_name)
      try:
        dbutils.fs.mount(
          source = "wasbs://{}@{}.blob.core.windows.net".format(source_mount, datalake_name),
          mount_point = mount_path,
          extra_configs = { 
            dlKey: datalake_key
          }
        )
        print("Mounting successful...")
      except Exception as e:
        print(e)
        failedMounts.append(mount)

# COMMAND ----------

storage_key = get_secret("kvdevaue-secret-scope","DATADEVAUE")

# COMMAND ----------

mount_storage(mount_path = "/mnt/data",  
      datalake_name = "datadevaue",
      datalake_key =  storage_key,
      source_mount = "data")

# COMMAND ----------

required_mounts = {"data":"/mnt/data", "government":"/mnt/government", "economy":"/mnt/economy", "policy":"/mnt/policy"}
datalake_name = "datadevaue"
all_mounts = [x[0] for x in dbutils.fs.mounts()]

for source_mount,mount_path in required_mounts.items():
  if mount_path not in all_mounts:
    mount_storage(mount_path = mount_path,  
      datalake_name = datalake_name,
      datalake_key =  get_secret("kvdevaue-secret-scope","DATADEVAUE"),
      source_mount = source_mount)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Hive Table Operations

# COMMAND ----------

# #sqlContext.sql("DROP TABLE productrecordlinklist")
# #dbutils.fs.rm('/mnt/productrecords-linklist/delta/',recurse=True)

# productListTableFilePath = '/mnt/productrecords-linklist/delta/'
# productRMSLinkTableFilePath = '/mnt/productrecords-rmslink/delta/'

# productListTableCreate = "CREATE TABLE IF NOT EXISTS productrecordlinklist (recordID int, recordLink string, specType string, code string, barcode string, retailerProductNumber string, recordReceived date, processStatus string) USING DELTA LOCATION '{0}' PARTITIONED BY (recordID)"
# sqlContext.sql(productListTableCreate.format(productListTableFilePath))

# productsRMSLinkTableCreate = "CREATE TABLE IF NOT EXISTS productrecordsrmslink (recordID int, recordLink string, barcode string, rmsId string, rmsIdUpdated date, recordReceived date, processStatus string) USING DELTA LOCATION '{0}' PARTITIONED BY (recordID)"
# sqlContext.sql(productsRMSLinkTableCreate.format(productRMSLinkTableFilePath))

# productListTableDelete = "DELETE FROM productrecordlinklist"
# sqlContext.sql(productListTableDelete)

# custom_schema_str = 'StructType(list([StructField("_ns1",StringType(),True),StructField("entries",ArrayType(StructType(list([StructField("_ns2",StringType(),True),StructField("_type",StringType(),True),StructField("_xsi",StringType(),True),StructField("recordId",LongType(),True),StructField("recordLink",StringType(),True),StructField("code",StringType(),True),StructField("title",StringType(),True)])),True),True),StructField("entryArray",ArrayType(StructType(list([StructField("recordId",LongType(),True),StructField("recordLink",StringType(),True),StructField("code",StringType(),True),StructField("title",StringType(),True)])),True),True),StructField("nextPage",StringType(),True),StructField("totalRecords",LongType(),True)]))'

# productListTableInsert = "INSERT INTO productrecordlinklist SELECT recordid AS recordID, recordLink, NULL as specType, code, NULL as barcode, NULL as retailerProductNumber, current_date() as recordReceived, NULL as processStatus FROM (SELECT inline(entries) from tempProdRecordLinkList)"

# productsRmsLinkTableInsert = "INSERT INTO productrecordsrmslink SELECT recordid AS recordID, recordLink, NULL as specType, code, NULL as barcode, NULL as rmsId, NULL as rmsIdUpdated, current_date() as recordReceived,  NULL as processStatus FROM (SELECT inline(entries) from tempProdRecordLinkList)"


# productListTableSelect = "SELECT recordLink from productrecordlinklist WHERE recordID = {0}"
# productListTableUpdate = "UPDATE productrecordlinklist SET barcode = '{0}', retailerProductNumber = '{1}' WHERE recordID in ({2})"

# productListTableUpdateStatus = "UPDATE productrecordlinklist SET processStatus = '{0}'"

# productRmsLinkTableDelete = "DELETE from productrecordsrmslink WHERE recordID = {0}"

# custom_schema = eval(custom_schema_str)
# root_tag = "ProductRecordLinkList"
# row_tag = "ProductRecordLinkList"
# file_path = "/FileStore/tables/temp/Product.xml"
# view_name = "tempProdRecordLinkList"
