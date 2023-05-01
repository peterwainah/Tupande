import unittest
import os
from pyspark.sql import SparkSession
from Ingestion import load_leads
## Initiate dotenvcd
load_dotenv()
os.environ['PYSPARK_PYTHON'] = sys.executable


class TestCSVIngestion(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("TestCSVIngestion").master("local[*]").getOrCreate()
        self.source_directory =os.getenv("source_directory")
        self.destination_directory = os.getenv("destination_directory")
        self.record_file = os.getenv("record_file")
        self.record_file = os.getenv("record_file")
        self.database_name = os.getenv("database_name")
        self.username = os.getenv("username")

    def tearDown(self):
        self.spark.stop()

    def test_ingest_csv_to_sql_server(self):
        # create a test CSV file
        test_df = self.spark.createDataFrame([(1, "John"), (2, "Jane"), (3, "Bob")], ["id", "name"])
        test_df.write.csv(f"{self.source_directory}/test.csv", header=True)

        # call the function
        TestCSVIngestion(self.source_directory, self.destination_directory, self.record_file,
                                 self.server_name, self.database_name, self.username, self.password)

        # check that the CSV file was ingested
        audit_df = self.spark.sql(f"SELECT * FROM {self.database_name}.audit")
        self.assertEqual(audit_df.count(), 1)
        self.assertEqual(audit_df.first().file_path, f"{self.source_directory}/test.csv")
        self.assertEqual(audit_df.first().row_count, 3)
        self.assertEqual(audit_df.first().status, "success")

        # delete the test CSV file and the record file
        shutil.rmtree(f"{self.destination_directory}/test.csv")
        os.remove(self.record_file)
