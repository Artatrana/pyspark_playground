from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, datediff
import logging
import sys 


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():

    try:

        # Initialize spark session
        #spark= SparkSession(appName = "EmployeeTenureApp")
        spark =  SparkSession.builder \
                .appName("EmployeeTenureApp") \
                .getOrCreate()

        #input the file
        input_path = "data/employee.csv"
        output_path = "data/processed_employees.csv"


        #Load data
        df = spark.read.option("header","true").csv(input_path)
        logger.info("Data successfully loaded from {}".format(input_path))

        # Trasfrom data - calculate employee tenure in years
        df_transformed = df.withColumn("tenure_in_days", datediff(current_date(), col("hire_date")))\
                            .withColumn("tenure_in_years", col("tenure_in_days") / 365)\
                            .drop("tenure_in_days")
        
        # Trasform - Filter employee having tenure >= 2 years
        #df_transformed = df_transformed.withColumn("tenure_in_years", col("tenure_in_years").cast("int"))
        #df_filtered = df_transformed.filter(col("tenure_in_years" >= 2))
        df_filtered = df_transformed.filter(col("tenure_in_years") >= 2)

        # Write output 
        df_filtered.write.mode("overwrite").csv(output_path, header=True)
        logger.info("Data successfully processed and written to {}".format(output_path))

    except Exception as e:
        logger.error("Error in processing: {}".format(e))
        sys.exit(1)

if __name__ == "__main__":
    main()

