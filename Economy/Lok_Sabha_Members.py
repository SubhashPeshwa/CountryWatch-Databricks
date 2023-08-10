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

resp.text

# COMMAND ----------


