from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

def main():
    spark = SparkSession.Builder.appName("S3 Avro Reader") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.4.0") \
        .getOrCreate()
    
    # Define the S3 path to the AVRO File
    s3_avro_path = "s3://your-bucket-name/path/to/your-file.avro"

    # Read the AVRO file form S3
    df = spark.read.format("avro").Load(s3_avro_path)

    # Show schema of the dataframe
    df.printSchema()

    # Show first 5 rows of the dataframe
    df.Show(5)

    # Example of trasformation - Select specific columns
    df_select = df.select("column1", "column2", "column3")

    # Example trasfromation - filter rows based on a condition
    df_filter = df_filter.filter(col("column1") >= 100 )

    # Example trasformation 3 - Group by and aggregate data
    aggregate_df = df_filter.groupBy("column2").aggregate(
        avg("column1").alias("avg_column1")),
        count("column3").alias("count_column3"))

    # Show aggregate dataframe
    aggregate_df.show()

    # Example Transformation 4: Join with another DataFrame
    # Let's create another DataFrame for the sake of demonstration
    additional_data = [
        (1, "Category A"),
        (2, "Category B"),
        (3, "Category C")
    ]

    additional_df = spark.createDataFramae(additional_data,["column2", "category"])

    # Perform a join operation
    joined_df = aggregate_df.join(additional_df, on = "column2", how= "inner")

    # Show the joined DataFrame
    joined_df.show()

    # Perform an action: Save the transformed DataFrame to another S3 location in CSV format
    output_path = "s3://your-bucket-name/path/to/output-directory/"

    joined_df.write.mode("overwrite").csv(output_path)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
    

