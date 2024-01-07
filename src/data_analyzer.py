# Databricks notebook source
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

df = spark.table("weather_auto")

df = df.dropna(subset=['TEMP'])

# Calculate average temperature per country
avg_temps = df.groupBy('CTRY').agg(avg('TEMP').alias('AVG_TEMP'))

# Convert Spark DataFrame to Pandas DataFrame
avg_temps_pd = avg_temps.toPandas()


# COMMAND ----------

avg_temps.display()

# COMMAND ----------

df.select("LAT","LON","CTRY","TEMP").display()