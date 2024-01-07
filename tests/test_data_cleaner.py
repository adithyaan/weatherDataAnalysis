# Databricks notebook source
# MAGIC %run ../src/data_cleaner

# COMMAND ----------

# MAGIC %run /weather/src/config

# COMMAND ----------

config = Config()

# COMMAND ----------

# MAGIC %fs ls /mnt/raw/silver/1929

# COMMAND ----------


import unittest


class TestApi(unittest.TestCase):
    def testApiCall(self):
        cleanDataFrame(1929)
        df = spark.read.format("csv").load(config.SILVER_PATH+"1929")
        self.assertEqual(df.isEmpty(),False)
        
    def testApiCallWithInvalidURL(self):
        cleanDataFrame(191919)
        self.assertEqual(os.path.exists(config.SILVER_PATH+"191919"),False)
    
        
        
unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(TestApi))


# COMMAND ----------


import unittest


class TestApi(unittest.TestCase):
    def testDataCleaner(self):
        cleanDataFrame(1929)
        df = spark.read.format("csv").load("/mnt/raw/1929/*.csv")
        self.assertEqual(df.isEmpty(),False)
        
    def testApiCallWithInvalidURL(self):
        # Ensure that the API call raises an exception
       
        downloadAndExtractGsodData('https://www1.ncdc.noaa.gov/pub/data/gsod_test', 2029, '/dbfs/mnt/raw/')

        # Assert that the path does not exist
        self.assertEqual(os.path.exists("/dbfs/mnt/raw/2029"),False)
        
        
unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(TestApi))
