from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime
# Create a SparkSession
spark = SparkSession.builder.appName("Create DataFrame").getOrCreate()
row_count = str(4)
file_path='c/c'
ingestion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
status="success"

# # Define the schema of the DataFrame
# schema = StructType([
#     StructField("file_path", StringType(), True),
#     StructField("row_count", IntegerType(), True),
#     StructField("status", StringType(), True),
#     StructField("ingestion_time", StringType(), True)
# ])

# # Create a list of tuples with the data
# data = [(file_path, int(row_count), status, ingestion_time)]
# print(data)

# # Create a DataFrame from the list of tuples and the schema
# df = spark.createDataFrame(data, schema=schema)

# # Show the DataFrame
# df.show()

# list of tuples of plants data
data = [("mango", "AP", "Guntur"),
        ("mango", "AP", "Chittor"),
        ("sugar cane", "AP", "amaravathi"),
        ("paddy", "TS", "adilabad"),
        ("wheat", "AP", "nellore")]
  
# giving column names of dataframe
columns = ["Crop Name", "State", "District"]
  
# creating a dataframe
dataframe = spark.createDataFrame(data, columns)
  
# show data frame
dataframe.show()