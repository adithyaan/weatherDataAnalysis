# Databricks notebook source
# MAGIC %md
# MAGIC # Weather Data Analysis
# MAGIC
# MAGIC ##Dependencies:
# MAGIC
# MAGIC - Python latest version
# MAGIC - Databricks Visualization: For Data Visualization
# MAGIC - pyspark - latest version
# MAGIC
# MAGIC ##Usage:
# MAGIC Autoloader to automatically detects the data generated by rest api and publishes to delta table
# MAGIC
# MAGIC ## Author Information:
# MAGIC Name: Adithya A N
# MAGIC Date: 5 JAN 2024
# MAGIC
# MAGIC Change Log:
# MAGIC     - 5 JAN 2024: Initial creation.

# COMMAND ----------

# MAGIC %run /weather/src/config

# COMMAND ----------

config_var = Config()

checkpointLocation = config_var.CHECKPOINT_LOCATION
silverPath = config_var.SILVER_PATH

# COMMAND ----------

# Define the schema for the DataFrame
schema = StructType([
    StructField("STN", StringType(), True),
    StructField("WBAN", IntegerType(), True),
    StructField("YEARMODA", IntegerType(), True),
    StructField("YEAR", IntegerType(), True),
    StructField("TEMP", DoubleType(), True),
    StructField("DEWP", DoubleType(), True),
    StructField("SLP", DoubleType(), True),
    StructField("STP", DoubleType(), True),
    StructField("VISIB", DoubleType(), True),
    StructField("WDSP", DoubleType(), True),
    StructField("MXSPD", DoubleType(), True),
    StructField("GUST", DoubleType(), True),
    StructField("MAX", DoubleType(), True),
    StructField("MIN", DoubleType(), True),
    StructField("PRCP", DoubleType(), True),
    StructField("SNDP", DoubleType(), True),
    StructField("FRSHTT", StringType(), True),
    StructField("CREATED_AT", TimestampType(), True),
    StructField("CTRY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("StationName", StringType(), True),
    StructField("LAT", StringType(), True), 
    StructField("LON", StringType(), True)
])

# COMMAND ----------

def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    """
    Load data from a specified cloud storage data source into a Spark table using structured streaming.

    Parameters:
    - data_source (str): The path to the cloud storage data source.
    - source_format (str): The format of the source data (e.g., "PARQUET").
    - table_name (str): The name of the Spark table to create or overwrite.
    - checkpoint_directory (str): The directory where Spark will write checkpoint data for fault-tolerance.

    Returns:
    - query: The Spark structured streaming query for loading data into the specified table.

    Example:
    Checkpoint_location = checkpointLocation
    target_table = "weather_auto"

    """

    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .option("rescudedDataColumn","_rescued_data")
                  .schema(schema)
                  .load(data_source)
                  .writeStream
                  .option("checkpointLocation", checkpoint_directory)
                  .table(table_name))
    return query

Checkpoint_location = checkpointLocation
target_table = "weather_auto"

query = autoload_to_table(data_source = silverPath + "*/*",
source_format = "PARQUET",
table_name = "weather_auto",
checkpoint_directory = f"{Checkpoint_location}/{target_table}")