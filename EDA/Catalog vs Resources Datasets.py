# Databricks notebook source
import os
import json
import requests
import delta
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, ArrayType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview of Bronze Layer catalog data

# COMMAND ----------

spark.sql("select * from default.data_gov_catalog d where `field_ministry_department:name` is not null union select COUNT(*) FROM default.data_gov_catalog d2;").display()
spark.sql("select count(*) from default.data_gov_resource_top where data_time_period_from is not null and datafile is not null and ministry_department is not null;").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## EDA

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catalog and Resource Data
# MAGIC
# MAGIC Findings: Catalog and Resources are heirarchical, Catalog is a collection of Resources joined together by uuid and catalog_uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common catalogs between most popular catalogs and resources
# MAGIC
# MAGIC - There is a strong correlation between the top catalogs and top resources (which makes sense as catalogs are made up of resources) - 1559 reources in total after filters and 1534 after joining
# MAGIC - Is the common dataset better than the individual catalog dataset?

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from
# MAGIC (
# MAGIC     select * from default.data_gov_catalog dc
# MAGIC     inner join default.data_gov_resource_top drt
# MAGIC     on dc.uuid[0] = drt.catalog_uuid[0]
# MAGIC     where  `field_group_name:name` is not null 
# MAGIC     and `field_ministry_department:name` is not null
# MAGIC     and data_time_period_from is not null 
# MAGIC     and datafile is not null
# MAGIC     -- and where drt.sector[0] = "All"
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Count of common catalogs per sector and ministry_department

# COMMAND ----------

# MAGIC %sql
# MAGIC select drt.sector, `field_ministry_department:name`, count(drt.sector) as count_sector from default.data_gov_catalog dc
# MAGIC inner join default.data_gov_resource_top drt
# MAGIC on dc.uuid[0] = drt.catalog_uuid[0]
# MAGIC -- where drt.sector[0] = "All"
# MAGIC group by drt.sector, `field_ministry_department:name`
# MAGIC order by count_sector desc;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resources that are popular, but are not part of the popular catalog list

# COMMAND ----------

# MAGIC %md
# MAGIC #### Which sectors have the best dataset - counts, views, downloads, age, last_updated

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct dc.`field_sector:name` from default.data_gov_catalog dc
# MAGIC inner join default.data_gov_resource_top drb
# MAGIC on dc.uuid[0] = drb.catalog_uuid[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Layer of data.gov catalog data
# MAGIC ### Build Catalog and Resources Processed tables
# MAGIC - Take ALL catalogs (pagination needed) ordered by views and store them in a processed catalogs table
# MAGIC   - Identify top and most recently updated datasets across sectors
# MAGIC   - Identify sectors that have the most usable data and drop the rest
# MAGIC - OPTIONAL - Take top resources and store them in a processed resources table, see if the top resources belong to any top catalogs.
# MAGIC - Extract resource data for each of the top catalogs across sectors (using either or both of the APIs)
# MAGIC - At the metadata level - Fact - Resources data from top catalogs, Dimensions - Catalog data, sectors list, 
# MAGIC - Create tables for each resource extracted - group tables together by catalog if necessary using partitions 
# MAGIC - At the data level, sector becomes a database, data for each catalog in the sector are stored as tables, all resources within a catalog are stored in the same table but partitioned by catalog_uuid (amongst other partition keys)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Identify non-array columns and flatten

# COMMAND ----------

# MAGIC %md
# MAGIC ### Choose the right columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Choose the right filters and save as Silver Layer table

# COMMAND ----------


