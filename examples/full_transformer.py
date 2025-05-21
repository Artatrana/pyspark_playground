from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, upper, current_timestamp,
    row_number, dense_rank, avg, count, sum as _sum
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql.window import Window

def full_data_transformer(df: DataFrame) -> DataFrame:
    # Step 1: Cast data types
    df = df.withColumn("created_at", col("created_at").cast(DateType())) \
           .withColumn("last_active_date", col("last_active_date").cast(TimestampType()))

    # Step 2: Add derived columns
    df = df.withColumn(
        "age_group",
        when(col("age") < 30, "Young")
        .when((col("age") >= 30) & (col("age") <= 40), "Adult")
        .otherwise("Senior")
    ).withColumn("country", upper(col("country"))) \
     .withColumn("record_inserted_at", current_timestamp())

    # Step 3: Filter data
    df = df.filter(col("purchase_amount") > 0)

    # Step 4: Window function - rank activities per user
    window_spec = Window.partitionBy("user_id").orderBy(col("last_active_date").desc())
    df = df.withColumn("activity_rank", row_number().over(window_spec))

    # Step 5: Deduplicate - keep only most recent activity per user
    df = df.filter(col("activity_rank") == 1)

    # Step 6: Aggregation - country level stats
    df_agg = df.groupBy("country").agg(
        count("*").alias("num_users"),
        avg("age").alias("avg_age"),
        _sum("purchase_amount").alias("total_purchase")
    ).orderBy("country")

    # Step 7: Action - show final data
    df_agg.show(truncate=False)
    print(f"Total countries: {df_agg.count()}")
    print("Collecting rows as Python objects:", df_agg.collect())

    return df_agg

if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("FullDataTransformer") \
        .getOrCreate()

    # Sample schema
    schema = StructType([
        StructField("user_id", StringType()),
        StructField("age", IntegerType()),
        StructField("country", StringType()),
        StructField("created_at", StringType()),
        StructField("last_active_date", StringType()),
        StructField("purchase_amount", DoubleType())
    ])

    # Sample data
    data = [
        ("u1", 25, "us", "2024-04-01", "2024-05-01 10:00:00", 100.0),
        ("u1", 25, "us", "2024-04-01", "2024-05-02 12:00:00", 200.0),
        ("u2", 35, "ca", "2024-03-15", "2024-05-01 09:00:00", 300.0),
        ("u3", 45, "de", "2024-03-10", "2024-04-20 14:00:00", 150.0),
        ("u4", 50, "us", "2024-02-01", "2024-03-01 08:00:00", 0.0),  # should be filtered
    ]

    # Create DataFrame
    df_input = spark.createDataFrame(data, schema=schema)

    # Run the transformation
    final_df = full_data_transformer(df_input)

    spark.stop()
