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
customers_df = spark.createDataFrame(customers_data, ["customer_id", "name", "signup_date", "city", "age"])
transactions_df = spark.createDataFrame(transactions_data, ["transaction_id", "customer_id", "transaction_date", "amount"])

# Filter customers over 30 years of age and living in New York
filtered_customers = customers_df.filter((col("age") > 30) & (col("city") == "New York"))

# Join filtered customers with their transaction history
joined_df = filtered_customers.join(transactions_df, on="customer_id")

# Aggregations: total spend and average transaction per customer
aggregated_df = joined_df.groupBy("customer_id", "name").agg(
    sum("amount").alias("total_spend"),
    avg("amount").alias("avg_transaction")
)

# Add customer ranking based on total spend using Window function
window_spec = Window.orderBy(desc("total_spend"))
ranked_df = aggregated_df.withColumn("rank", rank().over(window_spec))

# Show final ranked DataFrame
ranked_df.show()
