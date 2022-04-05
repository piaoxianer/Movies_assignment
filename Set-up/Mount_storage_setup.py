# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession

# COMMAND ----------

storage_account_name = "movieshopsrn"

# COMMAND ----------

# dbutils.fs.unmount("/mnt/movieshopsrn/bronze")

# COMMAND ----------

def mount_storage(container_name):
    dbutils.fs.mount(
      source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net":"1Ih/cvG6wUilpV3NrRcT3nnxFG7NiZwh4GAUOQvp4rZWUykVhpttZrqekEbFGdDnMxVQGIebuA+c+AStDm1lGg=="}
    )
# The key-name "1Ih/cvG6wUilpV3NrRcT3nnxFG7NiZwh4GAUOQvp4rZWUykVhpttZrqekEbFGdDnMxVQGIebuA+c+AStDm1lGg==" is the access key in the container in storage account

# COMMAND ----------

mount_storage('raw')

# COMMAND ----------

mount_storage('bronze')

# COMMAND ----------

mount_storage('silver')

# COMMAND ----------

dbutils.fs.ls('/mnt/movieshopsrn/raw')

# COMMAND ----------

dbutils.fs.ls('/mnt/movieshopsrn/bronze')

# COMMAND ----------

dbutils.fs.ls('/mnt/movieshopsrn/silver')