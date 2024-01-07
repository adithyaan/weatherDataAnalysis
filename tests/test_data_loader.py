# Databricks notebook source
# MAGIC %run ../src/data_loader

# COMMAND ----------


import unittest


class TestApi(unittest.TestCase):
    def testApiCall(self):
        downloadAndExtractGsodData('https://www1.ncdc.noaa.gov/pub/data/gsod', 1929, '/dbfs/mnt/raw/')
        df = spark.read.format("csv").load("/mnt/raw/1929/*.csv")
        self.assertEqual(df.isEmpty(),False)
        
    def testApiCallWithInvalidURL(self):
        # Ensure that the API call raises an exception
       
        downloadAndExtractGsodData('https://www1.ncdc.noaa.gov/pub/data/gsod_test', 2029, '/dbfs/mnt/raw/')

        # Assert that the path does not exist
        self.assertEqual(os.path.exists("/dbfs/mnt/raw/2029"),False)
    
    def testStationMappingFileAPI(self):
        downloadStationMappingFile()
        df = spark.read.format("csv").load("/mnt/raw/mapping/station_mapping.csv")
        self.assertEqual(df.isEmpty(),False)
        
        
unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(TestApi))
