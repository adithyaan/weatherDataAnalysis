# Databricks notebook source
import os
import requests
import tarfile
import gzip
from io import BytesIO
from pyspark.sql import SparkSession

class Config:
    def __init__(self) -> None:
        self.RAW_PATH = '/mnt/raw/'
        self.SILVER_PATH = self.RAW_PATH + 'silver/'
        self.API_URL = 'https://www1.ncdc.noaa.gov/pub/data/gsod'
        self.YEAR_LIST = list(range(1929,2024))
        self.FTP_URL = "ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv"
        self.STATION_MAPPING_FILE = '/mnt/raw/mapping/station_mapping.csv'
        self.CHECKPOINT_LOCATION = '/mnt/checkpoint'

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *