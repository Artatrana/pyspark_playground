from pyspark.sql import SparkSession

def get_spark_session(app_name: str) :
    """
    Initializes and returns a Spark session.
    :param app_name: Name of the Spark application.
    :return: SparkSession

    """

    return SparkSession.builder \
        .app_name(app_name) \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    