# Distributed-Demand-Forecasting-Pipeline
This project implements a Medallion Architecture using PySpark through a distributed environment (Spark), the system demonstrates the ability to handle massive datasets through strategic repartitioning, schema enforcement, and columnar storage (Parquet). The pipeline is containerized via Docker to ensure reproducibility.

## Pipeline Flow

```text
data/bronze/*.csv
  -> Phase 1: Bronze to Silver ingestion
  -> data/silver/sales_report/
  -> Phase 2: Silver to Gold feature engineering
  -> data/gold/demand_features/
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

## Running the Pipeline

Start the Spark cluster:

```bash
docker compose up -d
```

Run the full pipeline from the Spark master container:

```bash
docker exec -it <master-container-id> /opt/spark/bin/spark-submit /opt/spark/work-dir/main.py
```

Expected result:

- Phase 1 writes partitioned Silver Parquet to `data/silver/sales_report/`.
- Phase 2 writes partitioned Gold Parquet to `data/gold/demand_features/`.
- Each successful output directory contains a `_SUCCESS` marker file.
