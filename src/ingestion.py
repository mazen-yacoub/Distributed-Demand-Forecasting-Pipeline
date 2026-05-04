from pyspark.sql.functions import to_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.utils import get_spark_session 

def get_bronze_schema():
    """Defines the explicit schema for the Online Retail dataset."""
    return StructType([
        StructField("InvoiceNo", StringType(), True),   
        StructField("StockCode", StringType(), True),   
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", StringType(), True), 
        StructField("UnitPrice", DoubleType(), True),
        StructField("CustomerID", StringType(), True), 
        StructField("Country", StringType(), True)
    ])

def process_bronze_to_silver():
    spark = get_spark_session()
    
    # 1. Read Raw CSV (Bronze)
    # Using the path mapped in your docker-compose.yml
    raw_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(get_bronze_schema()) \
    .load("/opt/spark/work-dir/data/bronze/*.csv") 

    # 2. Cleaning & Type Casting
    # - Convert InvoiceDate from String to Timestamp
    # - Drop rows missing essential IDs
    # - Handle negative/zero quantities (common in retail data)
    cleaned_df = raw_df.withColumn(
            "InvoiceDate", 
            to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm")
        ) \
        .dropna(subset=["InvoiceNo", "StockCode"]) \
        .filter(col("Quantity") > 0) \
        .fillna({"CustomerID": "Unknown"})

    # 3. Write to Silver as Partitioned Parquet
    # We partition by Country to optimize geographic queries later
    cleaned_df.write.mode("overwrite") \
    .partitionBy("Country") \
    .parquet("/opt/spark/work-dir/data/silver/sales_report") 

    print(f"Success: Data moved to Silver Layer. Processed {cleaned_df.count()} rows.")

if __name__ == "__main__":
    process_bronze_to_silver()