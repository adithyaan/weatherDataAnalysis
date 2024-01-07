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
# MAGIC 1. downloadStationMappingFile function downloads station mapping file to map weather data with its station and country.
# MAGIC 2. downloadAndExtractGsodData function downloads weather data from GSOD rest and saves it in DBFS
# MAGIC 3. pass the filtered dataframe to aggregateData function to aggregate the data.
# MAGIC
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

# DBTITLE 1,Config class which has resusable functions and variables
# MAGIC %run /weather/src/config

# COMMAND ----------

# DBTITLE 1,Data Cleaner function to clean the data.
# MAGIC %run /weather/src/data_cleaner

# COMMAND ----------

# DBTITLE 1,Stage common variables
config_var = Config()

ftpUrl = config_var.FTP_URL
stationMappingFile = config_var.STATION_MAPPING_FILE

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Download the station mapping file from FTP server and save it as csv

# COMMAND ----------

from urllib.request import urlopen

# URL of the file on the FTP server
ftp_url = ftpUrl

def downloadStationMappingFile():
    '''
    Download the station mapping file from FTP server and save it as csv
    params: None
    return: None
    '''
    try:
    # Open the URL and read the content
        with urlopen(ftp_url) as f:
            file_content = f.read().decode('utf-8')

            # Print the content
            dbutils.fs.put(stationMappingFile, file_content, True)

    except Exception as e:
        print(f"An error occurred: {str(e)}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## This function downloads the data from GSOD rest api and saves the data into raw layer

# COMMAND ----------


def downloadAndExtractGsodData(apiUrl: str, year: int, downloadPath: str) -> None:
    '''
    Function to download and extract GSOD rest api data save it into raw layer
    params: dataDict: GSOD api url, year: weather data of a year, downloadPath: path where the downloaded data has to be saved
    return: None
    '''
    try:
        # Make a request to get the list of files in the specified year folder
        response = requests.get(f"{apiUrl}/{year}/")

        if response.status_code == 200:
            # Parse the HTML response to extract links to .tar files
            fileLinks = [line.split('"')[1] for line in response.text.split('\n') if '.tar' in line]

            # Create the local year folder if it doesn't exist
            localYearPath = downloadPath + str(year)
            os.makedirs(localYearPath, exist_ok=True)

            for fileLink in fileLinks:
                # Download the .tar file
                tarResponse = requests.get(apiUrl + f'/{year}/{fileLink}')

                if tarResponse.status_code == 200:
                    # Extract the contents of the .tar file
                    tarContent = BytesIO(tarResponse.content)
                    with tarfile.open(fileobj=tarContent, mode='r') as tar:
                        for member in tar.getmembers():
                            if member.name.endswith('.gz'):
                                # Extract and save the compressed file content to a CSV file
                                extractAndSaveCsv(tar.extractfile(member).read(), localYearPath, member.name, year)
                                print(f"Successfully extracted and saved {member.name} to {localYearPath}")
                                
                    cleanDataFrame(year)
                                
                else:
                    print(f"Failed to download {fileLink}. Status code: {tarResponse.status_code}")
        else:
            print(f"Failed to retrieve file list for {year}. Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred in the requests module: {str(e)}")

    except FileNotFoundError as e:
        print(f"File not found: {str(e)}")

    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

def extractAndSaveCsv(compressedData: bytes, localYearPath: str, memberName: str, year: str) -> None:
     
    '''
    Function to extract compresseddata and write it as csv
    params: compressedData: compressed data from GSOD rest api,localYearPath: path where data has to saved, memberName: file Name of data file from rest api, year: data for which year is downloaded
    return: None
    '''
    try:
        # Decompress the data using gzip and convert it to a CSV-formatted string
        with gzip.GzipFile(fileobj=BytesIO(compressedData), mode='rb') as f:
            csvData = f.read().decode('utf-8')

        # Save the CSV data to a file in DBFS
        dbfs_path = f"/mnt/raw/{year}/{memberName.replace('.', '')}test.csv"
        
        dbutils.fs.put(dbfs_path, csvData, True)

    except FileNotFoundError as e:
        print(f"File not found error while extracting and saving CSV: {str(e)}")

    except Exception as e:
        print(f"An unexpected error occurred while extracting and saving CSV: {str(e)}")