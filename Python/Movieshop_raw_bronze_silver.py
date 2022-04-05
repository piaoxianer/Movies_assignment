# Databricks notebook source
# MAGIC %md
# MAGIC # Raw Data Generation
# MAGIC ### Objective
# MAGIC 1. Mount 
# MAGIC 2. Ingest data from dbfs:/FileStore/tables/movieshop into our source directory, rawPath.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC #### Utility Function

# COMMAND ----------

# MAGIC %run ./includes/functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

# rm(dir: String, recurse: boolean = false): boolean -> Removes a file or directory
# dbutils.fs.rm(f"{bronzePath}/_delta_log/", recurse=True) 
# dbutils.fs.rm(f"{bronzePath}/", recurse=True) 
# dbutils.fs.rm(f"{bronzePath}/", recurse=True) 
# dbutils.fs.rm(f"{bronzePath}/", recurse=True) 
# dbutils.fs.rm(f"{bronzePath}/", recurse=True) 

# COMMAND ----------

# dbutils.fs.rm(f"{silverPath}/", recurse=True) 
# dbutils.fs.rm(f"{silverQuarantinePath}/", recurse=True) 

dbutils.fs.rm(f"{silverPath}/movies", recurse=True)
dbutils.fs.rm(f"{silverPath}/genres", recurse=True)
dbutils.fs.rm(f"{silverPath}/originalLanguage", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Raw Data Directory
# MAGIC There are eight files has landed in the Raw Data Directory.

# COMMAND ----------

# File location and type
# file_location = "dbfs:/FileStore/tables/movieshop/movie_0.json"
# file_location = "dbfs:/mnt/movieshopsrn/raw/"
# file_type = "json"
# file_name = "combined.delta"

# COMMAND ----------

# display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Print the Contents of the Raw Files

# COMMAND ----------

# print(dbutils.fs.head(dbutils.fs.ls(rawPath)[0].path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Raw to Bronze Pattern
# MAGIC ### Objective
# MAGIC 1. Augment the data with Ingestion Metadata
# MAGIC 2. Batch write the augmented data to a Bronze Table

# COMMAND ----------

# Read files from the source directory and write each line as a string to the Bronze table.
rawDF = read_batch_raw(rawPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Raw Data

# COMMAND ----------

# display(rawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestion Metadata

# COMMAND ----------

raw_metadata_df = transform_raw(rawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### WRITE Batch to a Bronze Table

# COMMAND ----------

raw_to_bronze = batch_writer(raw_metadata_df)
# raw_to_bronze.save(f"{bronzePath}/movies)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register the Bronze Table in the Metastore
# MAGIC 
# MAGIC The table should be named `health_tracker_classic_bronze`.

# COMMAND ----------

spark.sql("""
Drop Table If Exists health_tracker_bronze_movieshop
""")

spark.sql(f"""
CREATE TABLE health_tracker_bronze_movieshop
USING DELTA
LOCATION '{bronzePath}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display Classic Bronze Table

# COMMAND ----------

# %sql
# Select count(*) From health_tracker_bronze_movieshop

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select * From health_tracker_bronze_movieshop

# COMMAND ----------

# MAGIC %md
# MAGIC ### Purge Raw File Path

# COMMAND ----------

# dbutils.fs.rm(rawPath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze to Silver Pattern - ETL into a Silver table
# MAGIC ### Objective
# MAGIC 1. Develop the Bronze to Silver Step
# MAGIC    - Extract and transform the raw string to columns
# MAGIC    - Quarantine the bad data
# MAGIC    - Load clean data into the Silver table
# MAGIC 1. Update the status of records in the Bronze table

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load "New" Records from the Bronze Records

# COMMAND ----------

bronzeDF_new = read_batch_bronze()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extract the Nested JSON from the Bronze Records

# COMMAND ----------

silver_movies = transform_bronze_movies(bronzeDF_new)

# COMMAND ----------

movies_silver_clean, movies_silver_quarantine = generate_clean_and_quarantine_dataframes(silver_movies)

# COMMAND ----------

movies_silver_clean.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display the Quarantined Records

# COMMAND ----------

movies_silver_quarantine.count()

# COMMAND ----------

# display(movies_silver_quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a genres Silver Table from Movies silver table

# COMMAND ----------

silver_genres = silver_movies.select(explode("genres").alias("genres"),
                                           "Movies"
                                          )

# COMMAND ----------

silver_movies.count()

# COMMAND ----------

silver_genres.count()

# COMMAND ----------

silver_genres = silver_genres.select(col("genres.id").cast("integer").alias("genre_id"),
                                    col("genres.name").alias("genre_name"),
                                    col("Movies.Id").cast("integer").alias("Movie_Id"))

# COMMAND ----------

silver_genres = silver_genres.dropDuplicates().na.drop()
silver_genres = silver_genres.dropDuplicates().filter("Genre_name != '' ")

# COMMAND ----------

silver_genres.count()

# COMMAND ----------

# display(silver_genres)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a originallanguage Silver Table from Movies silver table

# COMMAND ----------

silver_originallanguage = silver_movies.select("Movie_Id","Title", "OriginalLanguage")

# COMMAND ----------

silver_originallanguage = silver_originallanguage.dropDuplicates().na.drop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write movies, genres, originalLanguage table to silver table

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write clean movies table to silver table

# COMMAND ----------

moviesTosilver_clean = batch_writer(dataframe=movies_silver_clean,
                                   exclude_columns=["Movies"])
moviesTosilver_clean.save(f"{silverPath}/movies")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write genres table to silver table

# COMMAND ----------

genresTosilver = batch_writer(dataframe=silver_genres)
genresTosilver.save(f"{silverPath}/genres")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write originalLanguage table to silver table

# COMMAND ----------

originallanguageToSilver = batch_writer(dataframe=silver_originallanguage)
originallanguageToSilver.save(f"{silverPath}/originalLanguage")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register the Tables in the Metastore

# COMMAND ----------

spark.sql("""
Drop Table If Exists health_tracker_silver_movies
""")

spark.sql(
    f"""
CREATE TABLE health_tracker_silver_movies
USING DELTA
LOCATION "{silverPath}/movies"
"""
)

# COMMAND ----------

# %sql
# Select * From health_tracker_silver_movies

# COMMAND ----------

spark.sql("""
Drop Table If Exists health_tracker_silver_genres
""")

spark.sql(
    f"""
CREATE TABLE health_tracker_silver_genres
USING DELTA
LOCATION "{silverPath}/genres"
"""
)

# COMMAND ----------

# %sql
# Select * From health_tracker_silver_genres

# COMMAND ----------

spark.sql("""
Drop Table If Exists health_tracker_silver_originallanguage
""")

spark.sql(
    f"""
CREATE TABLE health_tracker_silver_originallanguage
USING DELTA
LOCATION "{silverPath}/originalLanguage"
"""
)

# COMMAND ----------

# %sql
# Select * From health_tracker_silver_originallanguage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Bronze table to Reflect the Loads

# COMMAND ----------

update_bronze_table_status(spark, f"{bronzePath}/movies", movies_silver_clean, "loaded")
update_bronze_table_status(spark, f"{bronzePath}/movies", movies_silver_quarantine, "quarantined")

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Table Updates
# MAGIC ### Handle Quarantined Records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform the Quarantined Records
# MAGIC #### Find the quarantined records from bronze table

# COMMAND ----------

bronze_quarantinedDF = spark.read.table("health_tracker_bronze_movieshop").filter("status = 'quarantined'")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform those quarantined records

# COMMAND ----------

bronzeQuaTransDF = transform_bronze_movies(bronze_quarantinedDF, quarantine=True)

# COMMAND ----------

# display(bronzeQuaTransDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Write the Repaired (formerly Quarantined) Records to the Silver Table

# COMMAND ----------

bronzeToSilverWriter = batch_writer(
    dataframe=bronzeQuaTransDF, exclude_columns=["Movies"]
)
bronzeToSilverWriter.save(f"{silverPath}/movies")

update_bronze_table_status(spark, f"{bronzePath}/movies", bronzeQuaTransDF, "loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display the Quarantined Records
# MAGIC 
# MAGIC If the update was successful, there should be no quarantined records
# MAGIC in the Bronze table.

# COMMAND ----------

display(bronzeQuaTransDF)