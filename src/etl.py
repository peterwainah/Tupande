from typing import Tuple
import os
import sys
import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from dotenv  import load_dotenv
import logging
from pyspark.sql.types import StructType, StructField, IntegerType

## Initiate dotenvcd
load_dotenv()
os.environ['PYSPARK_PYTHON'] = sys.executable



def load_contract_offer(server_name: str, database_name: str, username: str, password: str,port: str) -> None:
    """
    Ingests CSV files from a source directory into a SQL Server database, and moves the ingested files to a
    destination directory. The function records the ingested file names in a record file, and updates an audit table
    with the file path, row count, status, and ingestion time.

    :param source_directory: The directory containing the CSV files to ingest.
    :param destination_directory: The directory to move the ingested CSV files to.
    :param record_file: The file to record ingested file names.
    :param server_name: The name of the SQL Server instance.
    :param port: The port of the SQL Server instance.
    :param database_name: The name of the SQL Server database.
    :param username: The username to use to connect to the SQL Server database.
    :param password: The password to use to connect to the SQL Server database.
    :return: None
    """
    
    try:
        
        # create a Spark session
        spark = SparkSession.builder.appName("CSV Ingestion").getOrCreate()
        # directory containing CSV files
        source_directory = os.getenv("source_directory")
        

        # directory to move CSV files to
        destination_directory = os.getenv("destination_directory")
        

        # file to record ingested files
        record_file = os.getenv("record_file")
        # list of CSV file names in the source directory
        csv_files = [f for f in os.listdir(source_directory) if f.endswith("offers.csv")]
        for csv_file in csv_files:
            # check if the file exists at the source directory
            if not os.path.isfile(f"{source_directory}/{csv_file}"):
                continue
        
        # read in the record file to a set
        if os.path.isfile(record_file):
            with open(record_file, "r") as f:
                ingested_files = set(f.read().splitlines())
        else:
            ingested_files = set()

        # loop through each CSV file and ingest it into a DataFrame
        for file in csv_files:
            if file not in ingested_files:
                file_path = os.path.join(source_directory, file)
                df = spark.read.csv(file_path, header=True, inferSchema=True)


                
                # write the DataFrame to SQL Server
                ingestion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                #add a timestamp column  
                df_with_timestamp = df.withColumn("ingestion_time", lit(ingestion_time))
                df_with_timestamp.write.format("jdbc") \
                    .option("url", f"jdbc:sqlserver://{server_name}:{port};database={database_name};encrypt=false") \
                    .option("dbtable", "stg.contract_offer")\
                    .option("user", username) \
                    .option("password", password) \
                    .mode("append") \
                    .save()
                
                # move the CSV file to the destination directory
                destination_path = os.path.join(destination_directory, file)
                shutil.move(file_path, destination_path)

                # add the ingested file to the record file
                with open(record_file, "a") as f:
                    f.write(file + "\n")
       
        # insert a new row into the audit table with success status and timestamp
        row_count = str(df_with_timestamp.count())
        ingestion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        if len(row_count) > 0:
            audit_tuple = (file_path, row_count, "success", ingestion_time)
            audit_df = spark.createDataFrame([audit_tuple], ["file_path", "row_count", "status", "ingestion_time"])
        else:
            print("No rows found in DataFrame")
        audit_df.show()

        audit_df.write.format("jdbc") \
            .option("url", f"jdbc:sqlserver://{server_name};database={database_name};encrypt=false") \
            .option("dbtable", "stg.audit") \
            .option("user", username) \
            .option("password", password) \
            .mode("append") \
            .save()

    except Exception as e:
        # log the error
        logging.error(f"An error occurred during ingestion: {str(e)}")

    finally:
        spark.stop()


if __name__ == "__main__":

    # SQL Server database credentials
    server_name = os.getenv("server_name")
    port = os.getenv("port")
    database_name = os.getenv("database_name")
    username = os.getenv("db_username")
    password = os.getenv("password")

load_contract_offer(server_name, database_name, username, password,port) 
    