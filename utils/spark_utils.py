from pyspark.sql import SparkSession


def get_spark_session(app_name):
    """
    Get a Spark session
    """
    return SparkSession.builder.appName(app_name).getOrCreate()
