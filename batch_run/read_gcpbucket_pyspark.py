from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, when

def main():
    # Initialize the spark session
    spark = SparkSession.builder \
        .appName("GCS Parquet Reader ") \
        .congig("spark.jars", "/path/to/gcs-connector-hadoop3-latest.jar") \
        .getOrCreate()
    
    # GCS path to the Parquet file
    gcs_parquet_path = "gs://your-bucket-name/path/to/your-file.parquet"

    # Read Parquet file from GCS
    df = spark.read.parquet(gcs_parquet_path)

    # Show schema and first few rrows
    df.printSchema()
    df.show()
    

    