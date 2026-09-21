# Redshift Serverless ETL Project

This project demonstrates a clean end-to-end ETL pattern for loading synthetic order and customer data into Amazon Redshift Serverless. It combines:

- synthetic data generation into Amazon S3
- a Lambda-based orchestration layer
- staging tables and copy commands in Redshift
- SCD Type 1 and Type 2 logic for the customer dimension and incremental fact processing

The design highlights a practical data warehouse pattern that is easy to understand and extend while modeling how real batch ingestion and historical dimension logic work together.

---

## Project purpose

The ETL process models the following business pattern:

- `orders` is a fact table that receives incremental updates over time
- `customers_snapshot` is a full snapshot loaded by date partition
- customer data is handled using SCD Type 2 logic
- order data is processed using a SCD Type 1-style upsert into the main `orders` table

The design mirrors a common data warehouse pattern where:

1. raw data is written to S3 in partitioned Parquet files
2. Redshift loads that data into staging tables
3. business logic transforms the staged data into final warehouse tables

---

## Folder contents

### `fake_data_dimensional_model.py`

This script generates synthetic data for two datasets:

- `orders`
- `customers_snapshot`

It writes the output as Parquet files into Amazon S3 under date-based partitions such as:

- `s3://<bucket>/orders/extract_date=<date>/`
- `s3://<bucket>/customers_snapshot/extract_date=<date>/`

Key behavior:

- `orders` is generated as an incremental fact dataset with overlapping IDs across loads
- `customers_snapshot` is a full snapshot, where each load includes all customer records up to the current point
- each dataset is split into multiple Parquet files per partition to simulate realistic batch ingestion

Important notes from the script:

- the S3 bucket name is hardcoded as `arshisreejitlove`
- the extraction date is controlled by the `extract_date` field in `control_configs`
- example load names include `inital_load`, `second_load`, `third_load`, and more

### `lambda_function.py`

This file is the ETL orchestration layer for AWS Lambda.

It performs the following sequence:

1. triggers the staging copy job from S3
2. waits for Redshift statement completion
3. if staging is successful, calls the `stage_to_main()` workflow
4. creates target tables and then calls stored procedures to load/merge data

The Lambda relies on the Amazon Redshift Data API (`boto3.client('redshift-data')`) and runs SQL statements asynchronously using `batch_execute_statement()`.

### `sql_code.py`

This file contains all the SQL used by the ETL process.

It includes:

- staging table creation for `orders_staging` and `customers_staging`
- truncate statements for staging tables
- COPY commands from S3 into staging
- target table creation for `orders` and `customers`
- stored procedures:
  - `scd1_upsert()` for the fact table
  - `scd2()` for the customer dimension history logic

---

## Data model

### Orders

The generated `orders` dataset contains fields such as:

- `order_id`
- `customer_id`
- `order_status`
- `order_amount`
- `order_ts`
- `updated_ts`
- `extract_date`

This is intentionally modeled so that `order_id` is unique within a single partition because the data for that partition is built as one logical snapshot of new and updated order activity. However, the same `order_id` can appear again in later partitions, which represents updates or repeat processing across time. This makes the pipeline a strong example of how incremental fact data works in data warehousing.

### Customers

The generated `customers_snapshot` dataset contains:

- `customer_id`
- `customer_name`
- `updated_ts`
- `extract_date`

This table is treated as a full snapshot, so later loads include the cumulative set of customers.

---

## ETL flow

### 1. Data generation

The script in `fake_data_dimensional_model.py` produces synthetic data and uploads parquet files to S3. The sample configuration creates a series of partitions such as:

- `extract_date=2026-09-19`
- `extract_date=2026-09-20`
- `extract_date=2026-09-21`

### 2. Staging load

The Redshift copy phase uses `COPY ... FROM 's3://...' FORMAT AS PARQUET` to load data into:

- `dev.public.orders_staging`
- `dev.public.customers_staging`

This is done by the Lambda orchestration logic through `copy_to_staging()`.

### 3. Transform and merge

The `stage_to_main()` function executes:

- creation of `orders` and `customers` tables
- creation of SCD procedures
- `CALL scd2();`
- `CALL scd1_upsert();`

### 4. Dimension handling

`scd2()` implements a Type 2 slowly changing dimension flow:

- marks the prior customer record as no longer current
- inserts a new row for the newest customer version
- preserves history using `start_timestamp` and `end_timestamp`
- ensures a `customer_sk = -99` placeholder record exists for unknown customers

### 5. Fact handling

`scd1_upsert()` merges order data into the `orders` table by `order_id` and looks up the current customer surrogate key from the latest customer dimension row.

This creates a fact table that reflects the latest state of each order and flags updates using `is_updated`.

---

## AWS setup assumptions

This example assumes the following AWS resources are configured:

- an S3 bucket with Parquet files written by the data generator
- an AWS Lambda function with permissions to call Redshift Data API
- an Amazon Redshift Serverless workgroup
- a target database named `dev`
- an IAM role that has Redshift access to read from S3

The code contains hardcoded values, including:

- bucket name
- Redshift workgroup ARN
- IAM role ARN
- database name

These values must be updated for your AWS account and environment.

---

## Required permissions

The project comments indicate the Lambda execution role should include permissions similar to:

- `AmazonRedshiftFullAccess`
- `AmazonRedshiftQueryEditorV2ReadWriteSharing`
- `AWSLambdaBasicExecutionRole`
- `AmazonS3FullAccess`
- `AmazonRedshiftDataFullAccess`

The IAM role also needs to be able to read from S3 and be mapped correctly to the Redshift database user.

---

## How to run

### 1. Install Python dependencies

Make sure the environment has the required libraries installed, including:

- `boto3`
- `pandas`
- `numpy`
- `pyarrow`
- `faker`

### 2. Generate the source data

Run the producer script:

```bash
python fake_data_dimensional_model.py
```

This writes parquet files into the configured S3 bucket and partition path.

### 3. Invoke the Lambda orchestration

The intended execution path is the Lambda handler in `lambda_function.py`:

```python
lambda_handler(event, context)
```

This triggers the staging load and then the final warehouse merge logic.

---

## Example data generation flow

The script includes example load definitions such as:

- initial load: customers and orders for early dates
- second load: overlapping orders and new customers
- later loads: additional incremental updates and snapshots

This simulates real-world batch intake and versioning behavior beautifully, especially the way order updates are handled across partitions while customer history is preserved through SCD logic.

---

## Summary

This project demonstrates a complete and practical ETL pattern in AWS:

- fake data generation in S3
- loading with Parquet and Redshift COPY
- dimension and fact pipeline design
- Lambda-driven orchestration
- historical customer tracking via SCD Type 2

It is especially useful for learning how batch ETL, dimensional modeling, and Redshift Serverless can work together in a real AWS architecture.
