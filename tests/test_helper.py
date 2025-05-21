import unittest
from pyspark.sql import SparkSession
from utils.helper import get_spark_session

class TestSparkSession(unittest.TestCase):

    def test_spark_session_createion(self):
        spark = get_spark_session(app_name="TestApp")
        self.assertIsInstance(spark, SparkSession)
        self.assertEqual(spark.sparkContext.app_name, "TestApp")

if __name__ == "__main__":
    unittest.main()


    
