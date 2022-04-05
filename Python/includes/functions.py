# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,
    explode,
    abs
)
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window
# from urllib.request import urlretrieve
from datetime import datetime

# COMMAND ----------

def read_batch_raw(rawPath: str) -> DataFrame:
    return (spark.read
            .option("multiLine", "true")
            .option("inferSchema", "true")
            .json("/mnt/movieshopsrn/raw")
            .select(explode("movie")
                    .alias("movies")
                   )
           )

# COMMAND ----------

def transform_raw(raw: DataFrame) -> DataFrame:
    return (raw.withColumn("datasource",lit("movieshop_assignment"))
            .withColumn("ingesttime", current_timestamp())
            .withColumn("status", lit("new"))
            .withColumn("p_ingestdate", current_timestamp().cast("date")))
        
# lit: Creates a Column of literal value.

# COMMAND ----------

def batch_writer(
    dataframe: DataFrame,
#     partition_column: str,
    exclude_columns: List = [],
    mode: str = "append",
) -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns
        )  # This uses Python argument unpacking (https://docs.python.org/3/tutorial/controlflow.html#unpacking-argument-lists)
        .write.format("delta")
        .mode(mode)
#         .partitionBy(partition_column)
#         .save(bronzePath)
    )

# COMMAND ----------

# TODO
def read_batch_bronze() -> DataFrame:
  return spark.read.table('health_tracker_bronze_movieshop').filter("status = 'new'")

# COMMAND ----------

def transform_bronze_movies(bronze: DataFrame, quarantine: bool = False) -> DataFrame:

#     bronzeAugmentedDF = bronze.withColumn(
#         "nested_json", from_json(col("value"), json_schema)
#     )

#     silver_movies = bronzeAugmentedDF.select("value", "nested_json.*")
    
    silver_movies = bronzeDF_new.select("Movies.Id","Movies.Title", "Movies.Overview", "Movies.Tagline", "Movies.Budget","Movies.Revenue","Movies.ImdbUrl","Movies.TmdbUrl","Movies.PosterUrl","Movies.BackdropUrl","Movies.OriginalLanguage","Movies.ReleaseDate","Movies.RunTime", "Movies.Price", "Movies.CreatedDate", "Movies.UpdatedDate", "Movies.UpdatedBy", "Movies.CreatedBy", "Movies.genres", "Movies")
    
    silver_movies = silver_movies.dropDuplicates()
    
    if not quarantine:
        silver_movies = silver_movies.select(
            col("Id").cast("integer").alias("Movie_Id"),
            "Title",
            "Overview",
            "Tagline",
            "Budget",
            "Revenue",
            "ImdbUrl",
            "TmdbUrl",
            "PosterUrl",
            "BackdropUrl",
            "OriginalLanguage",
            "ReleaseDate",
            col("Runtime").cast("integer").alias("Runtime"),
            "Price",
            "CreatedDate",
            "UpdatedDate",
            "UpdatedBy",
            "CreatedBy",
            "genres",
            "Movies"
        )
    else:
        silver_movies = silver_movies.withColumn("Budget", lit(1000000.0))
#         silver_movies = silver_movies.withColumn("Runtime", abs(silver_movies.Runtime))
        silver_movies = silver_movies.select(
            col("Id").cast("integer").alias("Movie_Id"),
            "Title",
            "Overview",
            "Budget",
            abs(col("Runtime").cast("integer")).alias("Runtime"),
            "Movies"
        )

    return silver_movies


# COMMAND ----------

def generate_clean_and_quarantine_dataframes(
    dataframe: DataFrame,
) -> (DataFrame, DataFrame):
    return (
        dataframe.filter(("Runtime >= 0") and ("Budget >= 1000000")),
        dataframe.filter(("Runtime < 0") or ("Budget < 1000000"))
    )

# COMMAND ----------

def repair_quarantined_records(
    spark: SparkSession, bronzeTable: str, userTable: str
) -> DataFrame:
    bronzeQuarantinedDF = spark.read.table(bronzeTable).filter("status = 'quarantined'")
    bronzeQuarTransDF = transform_bronze(bronzeQuarantinedDF, quarantine=True).alias(
        "quarantine"
    )
    health_tracker_user_df = spark.read.table(userTable).alias("user")
    repairDF = bronzeQuarTransDF.join(
        health_tracker_user_df,
        bronzeQuarTransDF.device_id == health_tracker_user_df.user_id,
    )
    silverCleanedDF = repairDF.select(
        col("quarantine.value").alias("value"),
        col("user.device_id").cast("INTEGER").alias("device_id"),
        col("quarantine.steps").alias("steps"),
        col("quarantine.eventtime").alias("eventtime"),
        col("quarantine.name").alias("name"),
        col("quarantine.eventtime").cast("date").alias("p_eventdate"),
    )
    return silverCleanedDF

# COMMAND ----------

def update_bronze_table_status(
    spark: SparkSession, bronzeTablePath: str, dataframe: DataFrame, status: str
) -> bool:

    bronzeTable = DeltaTable.forPath(spark, bronzePath)  # Create a DeltaTable for the data at the given path using the given SparkSession.
    dataframeAugmented = dataframe.withColumn("status", lit(status))

    update_match = "bronze_movies.Movies.Id = dataframe.Movie_Id"
    update = {"status": "dataframe.status"}

    (
        bronzeTable.alias("bronze_movies")
        .merge(dataframeAugmented.alias("dataframe"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

    return True

# COMMAND ----------

# def read_batch_delta(deltaPath: str) -> DataFrame:
#     return spark.read.format("delta").load(deltaPath)