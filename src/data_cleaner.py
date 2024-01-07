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
# MAGIC 1. cleanDataFrame -> This function cleans weather data from raw layer and publishes it to silver layer.
# MAGIC
# MAGIC ## Author Information:
# MAGIC Name: Adithya A N
# MAGIC Date: 5 JAN 2024
# MAGIC
# MAGIC ## Abstract/Description:
# MAGIC
# MAGIC
# MAGIC This code cleans weather data from raw layer and publishes it to silver layer.
# MAGIC
# MAGIC
# MAGIC Change Log:
# MAGIC     - 5 JAN 2024: Initial creation.

# COMMAND ----------

# MAGIC %run /weather/src/config

# COMMAND ----------

config_var = Config()

rawPath = config_var.RAW_PATH
stationMappingFile = config_var.STATION_MAPPING_FILE
silverPath = config_var.SILVER_PATH

# COMMAND ----------


def cleanDataFrame(year:str) -> None:
     
    '''
    Function to clean the data from raw layer and publish into silver layer.
    params: year: name of file in silver layer (one file for each year)
    return: None
    '''
    try:
        # Read CSV files into a DataFrame

        csv_df = spark.read.text(rawPath + str(year) + "/*")
        stationMappingDf = (
                spark.read.format("csv").option("header","true")
                                    .option("sep",",")
                                    .load(stationMappingFile)
                                    .select("USAF","CTRY","STATE",col("STATION Name").alias("StationName"),"LAT","LON")
                            )

        # Split the 'value' column into multiple columns using whitespace as the delimiter
        split_cols = split(csv_df['value'], '\s+')

        # Create new columns with appropriate names
        csv_df = csv_df.withColumn('STN', split_cols[0].cast("string"))
        csv_df = csv_df.withColumn('WBAN', split_cols[1].cast("integer"))
        csv_df = csv_df.withColumn('YEARMODA', split_cols[2].cast("integer"))
        csv_df = csv_df.withColumn('YEAR', lit(year))
        csv_df = csv_df.withColumn('TEMP', split_cols[3].cast("double"))
        csv_df = csv_df.withColumn('DEWP', split_cols[4].cast("double"))
        csv_df = csv_df.withColumn('SLP', split_cols[5].cast("double"))
        csv_df = csv_df.withColumn('STP', split_cols[6].cast("double"))
        csv_df = csv_df.withColumn('VISIB', split_cols[7].cast("double"))
        csv_df = csv_df.withColumn('WDSP', split_cols[8].cast("double"))
        csv_df = csv_df.withColumn('MXSPD', split_cols[9].cast("double"))
        csv_df = csv_df.withColumn('GUST', split_cols[10].cast("double"))
        csv_df = csv_df.withColumn('MAX', split_cols[11].cast("double"))
        csv_df = csv_df.withColumn('MIN', split_cols[12].cast("double"))
        csv_df = csv_df.withColumn('PRCP', split_cols[13].cast("double"))
        csv_df = csv_df.withColumn('SNDP', split_cols[14].cast("double"))
        csv_df = csv_df.withColumn('FRSHTT', split_cols[15].cast("string"))
        csv_df = csv_df.withColumn('CREATED_AT', current_timestamp())

        # Drop the original 'value' column
        csv_df = csv_df.drop('value')
        csv_df = csv_df.filter(col('YEARMODA').isNotNull())
        
        weatherCountryData = csv_df.alias("w").join(stationMappingDf.alias("S"),col("W.STN") == col("S.USAF"),"INNER").drop(*["USAF"])
        weatherCountryData.write.mode("overwrite").format("parquet").save(silverPath + str(year))
        print('write started')
    except Exception as ex:
        print(ex)
        raise ValueError(ex)        