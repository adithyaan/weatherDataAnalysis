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
# MAGIC 1. process_dataframe_with_threadpool -> implemented multithreading to efficicently ingestion using parallel processing. 
# MAGIC 2. data_transformer -> This function facilitates automatic loading of cloud storage data into a Spark table using structured streaming, with a focus on fault-tolerance through checkpointing
# MAGIC 3. pass the filtered dataframe to aggregateData function to aggregate the data.
# MAGIC
# MAGIC ## Author Information:
# MAGIC Name: Adithya A N
# MAGIC Date: 5 JAN 2024
# MAGIC
# MAGIC ## Abstract/Description:
# MAGIC
# MAGIC
# MAGIC This code aggregates sales data at product level for a year and presents the data.
# MAGIC
# MAGIC
# MAGIC Change Log:
# MAGIC     - 5 JAN 2024: Initial creation.

# COMMAND ----------

# MAGIC %run /weather/src/config

# COMMAND ----------

# MAGIC %run /weather/src/data_loader

# COMMAND ----------

# MAGIC %run /weather/src/data_transformer

# COMMAND ----------

config_var = Config()

rawPath = config_var.RAW_PATH
silverPath = config_var.SILVER_PATH
apiUrl = config_var.API_URL
yearList = config_var.YEAR_LIST

# COMMAND ----------

downloadStationMappingFile()

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark.sql import Row
import concurrent.futures
import os

# Function to process DataFrame using ThreadPoolExecutor
def process_dataframe_with_threadpool():
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
    # Use ThreadPoolExecutor to parallelize processing of DataFrame partitions
        futures = [executor.submit(downloadAndExtractGsodData, apiUrl, i, rawPath) for i in yearList]

        # Wait for all threads to complete
        concurrent.futures.wait(futures)

        # Get the results
        results = [future.result() for future in futures]


process_dataframe_with_threadpool()


# COMMAND ----------

# MAGIC %run /weather/src/data_analyzer

# COMMAND ----------

# %fs rm -r /user/hive/warehouse/weather_auto