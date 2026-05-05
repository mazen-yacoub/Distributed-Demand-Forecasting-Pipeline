import json
import os

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql.functions import col, datediff, lit

from src.utils import get_spark_session


GOLD_FEATURES_PATH = "/opt/spark/work-dir/data/gold/demand_features"
MODEL_OUTPUT_PATH = "/opt/spark/work-dir/models/gbt_demand_forecaster"
METRICS_OUTPUT_PATH = "/opt/spark/work-dir/models/gbt_demand_forecaster_metrics.json"

FEATURE_COLUMNS = [
    "lag_7_quantity",
    "lag_30_quantity",
    "rolling_7d_avg_quantity",
    "rolling_30d_avg_quantity",
    "rolling_7d_avg_revenue",
    "rolling_30d_avg_revenue",
]

LABEL_COLUMN = "daily_quantity"


def prepare_training_data(feature_store_df):
    """
    Prepares the Gold feature store for Spark MLlib.

    Spark ML models expect:

        label: numeric target column
        features: one vector column containing all input features

    We keep only past-looking lag and rolling features. Current-day columns such
    as daily_revenue, transaction_count, and unique_customers are useful for
    analytics, but they can leak same-day information into a forecasting model.
    """
    required_columns = FEATURE_COLUMNS + [LABEL_COLUMN, "sales_date"]

    return (
        feature_store_df.select(*required_columns)
        .dropna(subset=required_columns)
        .withColumn("label", col(LABEL_COLUMN).cast("double"))
        .withColumn("date_index", datediff(col("sales_date"), lit("1970-01-01")))
    )


def split_train_test_by_time(ml_df, train_ratio=0.8):
    """
    Splits rows chronologically instead of randomly.

    For forecasting, time-based evaluation is more realistic:

        train on earlier dates
        test on later dates

    The split point is based on the requested date percentile.
    """
    split_candidates = ml_df.approxQuantile("date_index", [train_ratio], 0.0)
    if not split_candidates:
        raise ValueError("No training rows are available after dropping null feature values.")

    split_point = split_candidates[0]
    train_df = ml_df.filter(col("date_index") <= split_point)
    test_df = ml_df.filter(col("date_index") > split_point)

    if train_df.rdd.isEmpty() or test_df.rdd.isEmpty():
        raise ValueError("Time-based split produced an empty train or test dataset.")

    return train_df, test_df, split_point


def build_gbt_pipeline():
    """
    Builds the Spark ML Pipeline.

    Pipeline stages:

        VectorAssembler -> GBTRegressor

    VectorAssembler converts multiple numeric feature columns into the single
    vector column required by Spark MLlib models.
    """
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
        handleInvalid="skip",
    )

    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        maxIter=30,
        maxDepth=5,
        seed=42,
    )

    return Pipeline(stages=[assembler, gbt])


def evaluate_predictions(predictions_df):
    """Calculates regression metrics at Spark scale."""
    rmse_evaluator = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="rmse",
    )
    mae_evaluator = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="mae",
    )

    return {
        "rmse": rmse_evaluator.evaluate(predictions_df),
        "mae": mae_evaluator.evaluate(predictions_df),
    }


def save_metrics(metrics):
    """Writes a small JSON metrics artifact beside the saved model."""
    output_dir = os.path.dirname(METRICS_OUTPUT_PATH)
    os.makedirs(output_dir, exist_ok=True)

    with open(METRICS_OUTPUT_PATH, "w", encoding="utf-8") as metrics_file:
        json.dump(metrics, metrics_file, indent=2)


def train_demand_forecasting_model():
    """
    Runs Phase 3 of the pipeline: Gold -> trained ML model.

    Output:
        /opt/spark/work-dir/models/gbt_demand_forecaster
        /opt/spark/work-dir/models/gbt_demand_forecaster_metrics.json
    """
    spark = get_spark_session("DemandForecastingML")

    feature_store_df = spark.read.parquet(GOLD_FEATURES_PATH)
    ml_df = prepare_training_data(feature_store_df)
    train_df, test_df, split_point = split_train_test_by_time(ml_df)

    pipeline = build_gbt_pipeline()
    model = pipeline.fit(train_df)

    predictions_df = model.transform(test_df)
    metrics = evaluate_predictions(predictions_df)
    metrics["train_rows"] = train_df.count()
    metrics["test_rows"] = test_df.count()
    metrics["time_split_date_index"] = split_point
    metrics["feature_columns"] = FEATURE_COLUMNS
    metrics["label_column"] = LABEL_COLUMN

    model.write().overwrite().save(MODEL_OUTPUT_PATH)
    save_metrics(metrics)

    print("Success: Trained Gradient Boosted Trees demand forecasting model.")
    print(f"Model saved to: {MODEL_OUTPUT_PATH}")
    print(f"Metrics saved to: {METRICS_OUTPUT_PATH}")
    print(f"RMSE: {metrics['rmse']}")
    print(f"MAE: {metrics['mae']}")


if __name__ == "__main__":
    train_demand_forecasting_model()
