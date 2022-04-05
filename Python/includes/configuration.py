# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC Define Data Paths.

# COMMAND ----------

# TODO
username = 'movieshopsrn'

# COMMAND ----------

classicPipelinePath = f"/mnt/{username}/"

rawPath = classicPipelinePath + "raw"
bronzePath = classicPipelinePath + "bronze"
silverPath = classicPipelinePath + "silver"
silverQuarantinePath = classicPipelinePath + "silverQuarantine"

# COMMAND ----------

# MAGIC %md
# MAGIC Configure Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS dbacademy_{username}")
spark.sql(f"USE dbacademy_{username}")

# COMMAND ----------

# MAGIC %md
# MAGIC Import Utility Functions

# COMMAND ----------

# %run ./utilities