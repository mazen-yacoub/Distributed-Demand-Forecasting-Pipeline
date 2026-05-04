from pyspark.sql import SparkSession

def get_spark_session(app_name="DemandForecasting"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()  # Use getOrCreate() instead of get_all()