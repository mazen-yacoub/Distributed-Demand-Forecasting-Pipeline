from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    current_date,
    lag,
    sum,
    to_date,
)

from src.utils import get_spark_session


SILVER_SALES_PATH = "/opt/spark/work-dir/data/silver/sales_report"
GOLD_FEATURES_PATH = "/opt/spark/work-dir/data/gold/demand_features"


def build_daily_demand(silver_df):
    """
    Converts cleaned invoice-level Silver data into daily demand rows.

    Forecasting models usually need one row per time period, not one row per
    transaction. Here, the grain becomes:

        Country + product_id + sales_date

    In this dataset, StockCode is the product identifier, so we expose it as
    product_id for clearer downstream modeling language.
    """
    enriched_df = silver_df.withColumn("product_id", col("StockCode")).withColumn(
        "sales_date", to_date(col("InvoiceDate"))
    ).withColumn("line_revenue", col("Quantity") * col("UnitPrice"))

    return (
        enriched_df.groupBy("Country", "product_id", "sales_date")
        .agg(
            sum("Quantity").alias("daily_quantity"),
            sum("line_revenue").alias("daily_revenue"),
            count("InvoiceNo").alias("transaction_count"),
            countDistinct("CustomerID").alias("unique_customers"),
        )
        .filter(col("sales_date").isNotNull())
    )


def add_time_series_features(daily_demand_df):
    """
    Adds lag and rolling-window features for demand forecasting.

    The rolling windows intentionally use previous rows only:

        rowsBetween(-7, -1)
        rowsBetween(-30, -1)

    This prevents data leakage because today's target value is not included in
    today's feature values.
    """
    series_window = Window.partitionBy("Country", "product_id").orderBy("sales_date")
    rolling_7d_window = series_window.rowsBetween(-7, -1)
    rolling_30d_window = series_window.rowsBetween(-30, -1)

    return (
        daily_demand_df.withColumn("lag_7_quantity", lag("daily_quantity", 7).over(series_window))
        .withColumn("lag_30_quantity", lag("daily_quantity", 30).over(series_window))
        .withColumn("rolling_7d_avg_quantity", avg("daily_quantity").over(rolling_7d_window))
        .withColumn("rolling_30d_avg_quantity", avg("daily_quantity").over(rolling_30d_window))
        .withColumn("rolling_7d_avg_revenue", avg("daily_revenue").over(rolling_7d_window))
        .withColumn("rolling_30d_avg_revenue", avg("daily_revenue").over(rolling_30d_window))
        .withColumn("feature_generated_date", current_date())
    )


def process_silver_to_gold():
    """
    Runs Phase 2 of the pipeline: Silver -> Gold.

    Output:
        /opt/spark/work-dir/data/gold/demand_features

    The Gold layer is a feature store table ready for modeling or analytics.
    """
    spark = get_spark_session()

    silver_df = spark.read.parquet(SILVER_SALES_PATH)

    daily_demand_df = build_daily_demand(silver_df)

    # Repartition by the same keys used in window operations to reduce shuffle
    # pressure when Spark builds per-product time series.
    optimized_df = daily_demand_df.repartition("Country", "product_id")

    feature_df = add_time_series_features(optimized_df)

    feature_df.write.mode("overwrite").partitionBy("Country").parquet(GOLD_FEATURES_PATH)

    print(f"Success: Data moved to Gold Layer. Created {feature_df.count()} feature rows.")


if __name__ == "__main__":
    process_silver_to_gold()
