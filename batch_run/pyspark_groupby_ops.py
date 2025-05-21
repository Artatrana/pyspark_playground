from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, desc, rank
from pyspark.sql.window import Window

# Start a Spark session
spark = SparkSession.builder.appName("ComplexTransformation").getOrCreate()

# Sample data: Customers and Transactions
customers_data = [
    (1, "Alice", "2020-01-01", "New York", 25),
    (2, "Bob", "2020-05-01", "Los Angeles", 32),
    (3, "Charlie", "2020-09-01", "Chicago", 22),
    (4, "David", "2020-03-15", "New York", 45),
    (5, "Eva", "2020-07-01", "San Francisco", 36)
]

transactions_data = [
    (1, 1, "2023-08-01", 250.0),
    (2, 1, "2023-08-10", 150.0),
    (3, 2, "2023-08-03", 400.0),
    (4, 3, "2023-08-20", 120.0),
    (5, 4, "2023-08-23", 500.0),
    (6, 4, "2023-09-01", 200.0),
    (7, 5, "2023-09-10", 300.0)
]

# Create DataFrames
# Create DataFrames
customers_df = spark.createDataFrame(customers_data, ["customer_id", "name", "signup_date", "city", "age"])
transactions_df = spark.createDataFrame(transactions_data, ["transaction_id", "customer_id", "transaction_date", "amount"])


#count: Counts the number of records for each group.
customers_df.groupBy("column").count()

#sum: Computes the sum of specified columns for each group.

customers_df.groupBy("column").sum("column_to_sum")

# avg / mean: Calculates the average value of specified columns for each group.

customers_df.groupBy("column").avg("column_to_avg")

# min: Finds the minimum value of specified columns within each group.
customers_df.groupBy("column").min("column_to_min")


#max: Determines the maximum value of specified columns within each group.
customers_df.groupBy("column").max("column_to_max")

# agg: Allows multiple aggregation functions on one or more columns within each group, using a dictionary format.
customers_df.groupBy("column").agg({"column_to_agg": "sum", "other_column": "avg"})

#sumDistinct: Computes the sum of distinct values in a column for each group.
customers_df.groupBy("column").sumDistinct("column_to_sum")


#countDistinct: Counts the distinct values in specified columns within each group.
customers_df.groupBy("column").countDistinct("column_to_count")

# first: Retrieves the first value in each group for the specified columns.
customers_df.groupBy("column").first("column_to_get_first")

# last: Retrieves the last value in each group for the specified columns.
customers_df.groupBy("column").last("column_to_get_last")