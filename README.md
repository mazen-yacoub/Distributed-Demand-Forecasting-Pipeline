# Distributed Demand Forecasting Pipeline

A containerized PySpark project that simulates a distributed data engineering workflow for retail demand forecasting. The project focuses on distributed processing, medallion architecture, schema enforcement, partitioned Parquet storage, Spark SQL transformations, and Spark MLlib execution inside Docker.

## Architecture

```text
Local project volume
  -> mounted into Spark containers at /opt/spark/work-dir

Bronze CSV
  -> Phase 1: ingestion and cleaning
  -> Silver partitioned Parquet
  -> Phase 2: feature engineering
  -> Gold feature store
  -> Phase 3: Spark ML pipeline
  -> trained model artifact
```

The local filesystem volume is used as an HDFS-style storage simulation for development. Spark master and worker containers read and write through the same mounted project directory, which keeps the pipeline reproducible without requiring a separate Hadoop cluster.

## Tech Stack

- Docker Compose
- Apache Spark 3.5.0
- PySpark
- Spark SQL
- Spark MLlib
- Parquet
- Python

## Project Structure

```text
.
├── data/
│   ├── bronze/                 # Raw input CSV
│   ├── silver/                 # Generated cleaned Parquet output
│   └── gold/                   # Generated feature store output
├── models/                     # Generated Spark ML model artifacts
├── src/
│   ├── ingestion.py            # Phase 1: Bronze -> Silver
│   ├── features.py             # Phase 2: Silver -> Gold
│   ├── ml_training.py          # Phase 3: Gold -> ML model
│   └── utils.py                # Spark session helper
├── Dockerfile                  # Project Spark image with Python deps
├── docker-compose.yml          # Spark master and worker services
├── main.py                     # Pipeline entrypoint
├── requirements.txt
└── README.md
```

`data/silver`, `data/gold`, and `models` are generated outputs and are ignored by Git.

## Phase 1: Bronze To Silver

Phase 1 reads raw Online Retail CSV data from:

```text
data/bronze/
```

It applies a strict schema contract, cleans the data, and writes partitioned Parquet to:

```text
data/silver/sales_report/
```

Main logic:

- Explicit `StructType` schema for stable ingestion.
- Timestamp conversion for `InvoiceDate`.
- Removal of rows missing critical identifiers.
- Removal of non-positive quantities to focus on sales demand.
- Missing customer handling with `Unknown`.
- Parquet output partitioned by `Country`.

## Phase 2: Silver To Gold

Phase 2 transforms cleaned invoice-level data into a forecasting-ready feature store.

Output:

```text
data/gold/demand_features/
```

The feature store grain is:

```text
Country + product_id + sales_date
```

`StockCode` is treated as `product_id`.

Daily demand metrics:

- `daily_quantity`
- `daily_revenue`
- `transaction_count`
- `unique_customers`

Time-series features:

- `lag_7_quantity`
- `lag_30_quantity`
- `rolling_7d_avg_quantity`
- `rolling_30d_avg_quantity`
- `rolling_7d_avg_revenue`
- `rolling_30d_avg_revenue`

The rolling windows use previous records only, which avoids using current-day demand inside current-day model features.

Before window calculations, the data is repartitioned by:

```text
Country, product_id
```

This aligns physical distribution with the time-series window partitions.

## Phase 3: Scalable ML Pipeline

Phase 3 trains a Spark MLlib regression pipeline on the Gold feature store.

Output:

```text
models/gbt_demand_forecaster/
models/gbt_demand_forecaster_metrics.json
```

Pipeline stages:

```text
VectorAssembler -> GBTRegressor
```

The ML stage demonstrates distributed feature vectorization, model training, chronological train/test splitting, and distributed regression evaluation. The model is included as a final intelligence layer on top of the data platform; the main emphasis of this project is the scalable data pipeline and reproducible Spark environment.

## Running The Project

Build the project Spark image:

```bash
docker compose build
```

Start the Spark cluster:

```bash
docker compose up -d
```

Check the Spark master container:

```bash
docker ps
```

Run the full pipeline:

```bash
docker exec -it <spark-master-container> /opt/spark/bin/spark-submit /opt/spark/work-dir/main.py full
```

Run a single phase:

```bash
docker exec -it <spark-master-container> /opt/spark/bin/spark-submit /opt/spark/work-dir/main.py phase1
docker exec -it <spark-master-container> /opt/spark/bin/spark-submit /opt/spark/work-dir/main.py phase2
docker exec -it <spark-master-container> /opt/spark/bin/spark-submit /opt/spark/work-dir/main.py phase3
```

Example container name from Docker Compose:

```text
distributed-demand-forecasting-pipeline-spark-master-1
```

## Inspecting Outputs

Preview the Gold feature store with PySpark:

```bash
docker exec -it <spark-master-container> /opt/spark/bin/pyspark
```

Then inside PySpark:

```python
df = spark.read.parquet("/opt/spark/work-dir/data/gold/demand_features")
df.printSchema()
df.show(10, truncate=False)
```

Check the trained model artifact:

```text
models/gbt_demand_forecaster/
```

## Notes

- The Spark cluster is simulated locally with Docker Compose.
- The project directory is mounted into Spark containers to simulate shared distributed storage.
- Parquet outputs and model artifacts are generated by the pipeline and are not intended to be committed.
- If publishing without the raw dataset, place a compatible Online Retail CSV file under `data/bronze/` before running the pipeline.
