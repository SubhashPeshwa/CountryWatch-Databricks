# Databricks notebook source
import json
import requests

# COMMAND ----------

def call_data_gov_api(uri):
    url = "https://api.data.gov.in"
    uri = "/resource/81153f15-b4da-45b5-a299-f307351c5001"
    api_key = ""
    resp_format = "json"
    limit = 500
    offset = 0

    resp = requests.get("{}{}?api-key={}&format={}&limit={}&offset={}".format(url,uri,api_key,resp_format,limit,offset))

# COMMAND ----------

uri = "/resource/81153f15-b4da-45b5-a299-f307351c5001"

resp = requests.get("{}{}?api-key={}&format={}&limit={}&offset={}".format(url,uri,api_key,resp_format,limit,offset))

# COMMAND ----------



# COMMAND ----------


