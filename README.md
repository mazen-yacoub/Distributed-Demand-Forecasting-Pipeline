# Distributed-Demand-Forecasting-Pipeline
This project implements a Medallion Architecture using PySpark through a distributed environment (Spark), the system demonstrates the ability to handle massive datasets through strategic repartitioning, schema enforcement, and columnar storage (Parquet). The pipeline is containerized via Docker to ensure reproducibility.

## Pipeline Flow

```text
data/bronze/*.csv
  -> Phase 1: Bronze to Silver ingestion
  -> data/silver/sales_report/
  -> Phase 2: Silver to Gold feature engineering
  -> data/gold/demand_features/
  -> Phase 3: scalable ML training
  -> models/gbt_demand_forecaster/
```

## Phase 1: Bronze to Silver

Phase 1 reads the raw Online Retail CSV files from the Bronze layer, applies a strict schema, cleans the transactional data, and writes the result as partitioned Parquet.

Main module:

```text
src/ingestion.py
```

Key transformations:

- Enforces an explicit schema for invoice, product, customer, country, quantity, and price fields.
- Converts `InvoiceDate` from string to timestamp.
- Drops rows missing required identifiers: `InvoiceNo` and `StockCode`.
- Removes non-positive quantities so the pipeline focuses on sales demand.
- Fills missing `CustomerID` values with `Unknown`.
- Writes Silver Parquet partitioned by `Country`.

Silver output:

```text
data/silver/sales_report/
```

## Phase 2: Silver to Gold

Phase 2 converts cleaned invoice-level data into a forecasting-ready feature store.

Main module:

```text
src/features.py
```

The Silver data is still transactional, meaning one product can appear many times on the same day. Forecasting needs a stable time-series grain, so Phase 2 first aggregates the data into daily demand:

```text
Country + product_id + sales_date
```

In the Online Retail dataset, `StockCode` is used as the product identifier and is renamed to `product_id` for modeling clarity.

Daily demand columns:

- `Country`
- `product_id`
- `sales_date`
- `daily_quantity`
- `daily_revenue`
- `transaction_count`
- `unique_customers`

Feature columns:

- `lag_7_quantity`
- `lag_30_quantity`
- `rolling_7d_avg_quantity`
- `rolling_30d_avg_quantity`
- `rolling_7d_avg_revenue`
- `rolling_30d_avg_revenue`
- `feature_generated_date`

The rolling windows use previous rows only, so the current day's target value is not included in the current day's features. This avoids data leakage when the Gold layer is used for forecasting.

Before calculating time-series features, the daily demand data is repartitioned by:

```text
Country, product_id
```

This matches the window partition keys and helps Spark distribute per-product time-series work more efficiently.

Gold output:

```text
data/gold/demand_features/
```

## Phase 3: Scalable ML Pipeline

Phase 3 trains a distributed Spark ML model on the Gold feature store.

Main module:

```text
src/ml_training.py
```

Input:

```text
data/gold/demand_features/
```

Outputs:

```text
models/gbt_demand_forecaster/
models/gbt_demand_forecaster_metrics.json
```

### Prediction Target

The model predicts:

```text
daily_quantity
```

Inside Spark MLlib, this target is stored as:

```text
label
```

So the model learns:

```text
Given historical product demand behavior, predict today's demand quantity.
```

### ML Feature Selection

The model uses only past-looking features:

- `lag_7_quantity`
- `lag_30_quantity`
- `rolling_7d_avg_quantity`
- `rolling_30d_avg_quantity`
- `rolling_7d_avg_revenue`
- `rolling_30d_avg_revenue`

Current-day analytics columns such as `daily_revenue`, `transaction_count`, and `unique_customers` are not used as model inputs. They are useful for reporting, but they may leak information that would not be available before the prediction day.

Rows with missing lag or rolling values are dropped before training. This usually removes the earliest rows in each product-country time series, because those rows do not yet have enough history.

### Vectorization

Spark MLlib estimators expect model inputs in one vector column named:

```text
features
```

Phase 3 uses `VectorAssembler` to convert multiple numeric columns into that single vector:

```text
lag_7_quantity, lag_30_quantity, rolling_7d_avg_quantity, ...
  -> features
```

### Model

The training pipeline uses:

```text
VectorAssembler -> GBTRegressor
```

`GBTRegressor` means Gradient Boosted Trees for regression. It is a strong first model for tabular forecasting features because it can learn nonlinear relationships between recent demand patterns and future quantity.

### Evaluation

The data is split chronologically:

```text
earlier dates -> train set
later dates   -> test set
```

This is better than a random split for forecasting because the model is evaluated on future-like data.

Phase 3 calculates:

- `RMSE`: root mean squared error. Larger mistakes are penalized more.
- `MAE`: mean absolute error. Easier to interpret as average units of demand error.

Both metrics are calculated with Spark MLlib's `RegressionEvaluator`, so evaluation runs in a distributed way.

## Running the Pipeline

Start the Spark cluster:

```bash
docker compose build
docker compose up -d
```

`docker compose build` creates the project Spark image and installs Python dependencies from `requirements.txt`. Phase 3 needs `numpy` because Spark MLlib imports it internally.

Run one pipeline stage from the Spark master container:

```bash
docker exec -it <master-container-id> /opt/spark/bin/spark-submit /opt/spark/work-dir/main.py phase1
docker exec -it <master-container-id> /opt/spark/bin/spark-submit /opt/spark/work-dir/main.py phase2
docker exec -it <master-container-id> /opt/spark/bin/spark-submit /opt/spark/work-dir/main.py phase3
```

Run the full reproducible pipeline:

```bash
docker exec -it <master-container-id> /opt/spark/bin/spark-submit /opt/spark/work-dir/main.py full
```

Expected result:

- Phase 1 writes partitioned Silver Parquet to `data/silver/sales_report/`.
- Phase 2 writes partitioned Gold Parquet to `data/gold/demand_features/`.
- Phase 3 writes a trained model to `models/gbt_demand_forecaster/`.
- Phase 3 writes metrics to `models/gbt_demand_forecaster_metrics.json`.
- Successful Spark output directories contain a `_SUCCESS` marker file.
