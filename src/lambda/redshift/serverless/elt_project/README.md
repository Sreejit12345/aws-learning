# Redshift Serverless ELT Project

This project loads synthetic order and customer data into Amazon Redshift Serverless using an ELT flow. The data is first written to S3, then copied into Redshift staging tables, and finally transformed into the warehouse tables.

It includes:

- synthetic data generation into S3
- a Lambda job to orchestrate the load
- staging tables for raw ingestion
- SCD Type 1 and SCD Type 2 logic for the final warehouse tables

---

## What this project is doing

The flow is meant to mirror a simple data warehouse pattern:

- raw files are written to S3 by date partition
- Redshift loads those files into staging tables
- the warehouse logic updates the final tables

The main business pattern here is:

- `orders` is an incremental fact table
- `customers_snapshot` is a full snapshot by extract date
- customer history is handled with SCD Type 2
- order updates are handled with SCD Type 1 style logic

### Supported load mode

This project is designed for **incremental loads**, not a full historical reload. Each run loads one order partition and one customer snapshot partition. The `orders` table keeps one current row per `order_id`, and repeated order IDs are treated as updates. The fact load therefore uses `is_latest = true` to assign the customer's latest surrogate key.

The customer source is a full snapshot for each batch, but that does not make the warehouse process a full load. The snapshot is used to detect the current customer version while previously loaded customer versions are preserved by the SCD2 logic.

To support a true full historical load, the following changes are required:

- Read and process all source partitions in chronological order.
- Deduplicate each partition and define how repeated `order_id` and `customer_id` records are selected before running `MERGE`. A full set of partitions contains duplicate business keys.
- Change the fact model if order history must be preserved. The current `orders` table has one row per `order_id` and represents the latest order state; it cannot store multiple historical versions of one order.
- Use reliable source-effective timestamps for customer SCD2 intervals. The point-in-time lookup must use a predicate such as `order_ts >= start_timestamp` and `order_ts < end_timestamp`, without filtering on `is_latest`.
- Make the first customer version start at an appropriate business date and ensure every subsequent interval is contiguous and non-overlapping. Using `1900-01-01` is only valid when the customer is known to have existed from that date.
- Make the load idempotent and protect against older partitions overwriting newer order versions by comparing source and target `updated_ts` values.

---

## Project purpose

The idea is to simulate a realistic batch ingestion pattern:

- a daily extract lands in S3
- the next step loads that raw data into staging tables
- the final step applies the transformation logic in Redshift


---

## Folder contents

### `fake_data_dimensional_model.py`

This script generates two datasets:

- `orders`
- `customers_snapshot`

It writes them to S3 as Parquet files partitioned by date, for example:

- `s3://<bucket>/orders/extract_date=<date>/`
- `s3://<bucket>/customers_snapshot/extract_date=<date>/`

The important part is the pattern:

- `orders` is incremental and contains overlapping IDs across loads
- `customers_snapshot` is a full snapshot for each extract date
- each partition is split into multiple files to simulate a real batch load

The bucket name and extract dates are hardcoded in the sample project.
### `lambda_function.py`

This file is the Lambda orchestration layer.

The job does this in order:

1. reads the batch date from the event
2. computes the previous day’s S3 partition
3. copies the raw Parquet files into Redshift staging tables
4. runs the transformation logic to load the final warehouse tables

It uses the Redshift Data API (`boto3.client('redshift-data')`) and executes SQL asynchronously with `batch_execute_statement()`.

### `sql_code.py`

This file contains the SQL used by the load and transformation steps.

It includes:

- staging table creation for `orders_staging` and `customers_staging`
- truncate statements for staging tables
- `COPY` commands from S3 into Redshift
- final target table creation for `orders` and `customers`
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

This is intentionally modeled so that `order_id` is unique within a single partition, because each partition represents one snapshot of that batch. The same `order_id` can appear again in later partitions, which is how updates are represented over time. That is a good example of how incremental fact data works in a warehouse.

### Customers

The generated `customers_snapshot` dataset contains:

- `customer_id`
- `customer_name`
- `updated_ts`
- `extract_date`

This table is treated as a full snapshot, so later loads include the cumulative set of customers.

---

## ELT flow

### 1. Data generation

The script in `fake_data_dimensional_model.py` produces synthetic data and uploads Parquet files to S3. The sample config creates a series of partitions like this:

- `extract_date=2026-09-19`
- `extract_date=2026-09-20`
- `extract_date=2026-09-21`

### 2. Staging load

The next step is the Redshift copy phase. It uses `COPY ... FROM 's3://...' FORMAT AS PARQUET` to load data into:

- `dev.public.orders_staging`
- `dev.public.customers_staging`

This is done in the Lambda flow through `copy_to_staging()`.

### Run date and S3 partition logic

The Lambda is designed to receive a `run_date` value from the event payload. That value represents the batch execution date, while the actual S3 partition that gets loaded is always the previous day.

In practice, the logic works like this:

- if `run_date = 2026-09-20`, the Lambda reads from the S3 partition for `2026-09-19`
- the code subtracts one day before building the `COPY` path
- this creates a clean batch pattern where the pipeline processes the previous day’s extracted data

Example:

```python
run_date = "2026-09-20"
read_date = (datetime.datetime.strptime(run_date, "%Y-%m-%d").date() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
```

That results in:

```text
2026-09-19
```

and the Lambda reads from the corresponding S3 partition:

```text
s3://<bucket>/orders/extract_date=2026-09-19/
s3://<bucket>/customers_snapshot/extract_date=2026-09-19/
```

This pattern keeps the data pipeline aligned with the extract date, while the Lambda run date is used as the scheduler or orchestration date.

### 3. Transform and merge

After the staged data is loaded for the previous date’s partition, the `stage_to_main()` function executes:

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

> **SCD2 surrogate key note:** The current `is_latest = true` lookup is correct for this incremental design. Each batch either inserts a new order or updates an existing order, and both cases should use the customer's latest surrogate key.
>
> If the pipeline is changed to perform a full historical load, where all partitions are read and reprocessed, the lookup in `#keychange` must be point-in-time instead. This preserves the customer SK that was valid when each order occurred:
>
> ```sql
> JOIN customers c
>   ON o.customer_id = c.customer_id
>  AND o.order_ts >= c.start_timestamp
>  AND o.order_ts < c.end_timestamp
> ```
>
> The historical lookup should not filter on `c.is_latest = true`, because older orders may need an expired customer dimension version.

### Example customer order totals

For the current incremental design, join the fact table to the dimension using the stored surrogate key. This preserves the customer version assigned to each order:

```sql
SELECT
  c.customer_id,
  c.customer_name,
  SUM(o.order_amount) AS total_order_amount
FROM orders o
JOIN customers c
  ON o.customer_sk = c.customer_sk
GROUP BY
  c.customer_id,
  c.customer_name
ORDER BY
  c.customer_id;
```
 
 Grouping by `customer_name` can produce separate rows when a customer's name changed; group by `customer_id` when totals across all customer versions are required.

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

These values must be updated for the AWS account and environment.

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

### 4. Deploy the Lambda package

To deploy this project as an AWS Lambda function, zip the contents of the `elt_project` folder and upload that zip file to the Lambda function.

From the project folder, run:

```bash
zip -r dep.zip .
```

This zip should include the Python files used by the Lambda, especially:

- `lambda_function.py`
- `sql_code.py`

Then upload `dep.zip` in the AWS Lambda console or with the AWS CLI.


This keeps the Lambda code and SQL logic together as a single deployable package.

---

## Example data generation flow

The script includes example load definitions such as:

- initial load: customers and orders for early dates
- second load: overlapping orders and new customers
- later loads: additional incremental updates and snapshots

This simulates real-world batch intake and versioning behavior, especially the way order updates are handled across partitions while customer history is preserved through SCD logic.

---

## Summary

This project shows a practical ELT pattern in AWS:

- data is generated and landed in S3
- Redshift loads the raw files into staging
- the final warehouse tables are built from those staging loads
- Lambda controls the orchestration
- customer history is tracked through SCD Type 2

It is a good example to demonstrate how incremental fact loads and dimension history work together in Redshift Serverless.
