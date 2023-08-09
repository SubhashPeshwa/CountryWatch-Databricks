# Databricks notebook source

#Storage account and key you will get it from the portal as shown in the Cookbook Recipe.
storageAccount=""
storageKey ="xx-xx-xxx"
mountpoint = "/mnt/Blob"
storageEndpoint =   "wasbs://rawdata@{}.blob.core.windows.net".format(storageAccount)
storageConnSting = "fs.azure.account.key.{}.blob.core.windows.net".format(storageAccount)

try:
  dbutils.fs.mount(
  source = storageEndpoint,
  mount_point = mountpoint,
  extra_configs = {storageConnSting:storageKey})
except:
    print("Already mounted...."+mountpoint)
