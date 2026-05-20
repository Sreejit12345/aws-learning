'''

Script to generate fake files and upload to s3 bucket to explore automated.


orders data is partitioned by year,month,day. It is not present directly in file but as partition columns, when reading it 
adds those columns.

country indicator is non partitoned.


We have 1 bucket: orders123456sree under which 2 subfolders.

sourceA/    -year/month/day  -- partitions

sourceB/    - direct files

'''

from faker import Faker
import pandas as pd
import random
import uuid
from datetime import datetime, timedelta
import boto3
from io import StringIO

fake = Faker()

# =========================================================
# CONFIG
# =========================================================

NUM_ORDER_RECORDS = 25
NUM_COUNTRY_RECORDS = 16

# =========================================================
# ORDER DATASET CONFIG
# =========================================================

PRODUCT_CATALOG = {
    "Electronics": ["Laptop", "Smartphone", "Tablet", "Headphones", "Smartwatch"],
    "Clothing": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Hoodie"],
    "Home": ["Chair", "Table", "Lamp", "Sofa", "Curtains"],
    "Books": ["Novel", "Notebook", "Biography", "Comics", "Magazine"],
    "Groceries": ["Rice", "Milk", "Bread", "Eggs", "Coffee"]
}

# =========================================================
# COUNTRY DATASET CONFIG
# =========================================================

COUNTRIES = [
    {"country": "India", "country_code": "IN", "continent": "Asia", "population": 1428627663},
    {"country": "United States", "country_code": "US", "continent": "North America", "population": 339996563},
    {"country": "Germany", "country_code": "DE", "continent": "Europe", "population": 83294633},
    {"country": "Brazil", "country_code": "BR", "continent": "South America", "population": 216422446},
    {"country": "Australia", "country_code": "AU", "continent": "Oceania", "population": 26439111},
    {"country": "South Africa", "country_code": "ZA", "continent": "Africa", "population": 60414495},
]

INDICATORS = [
    "COVID_CASES",
    "COVID_DEATHS",
    "VACCINATIONS",
    "FLU_CASES",
    "MALARIA_CASES"
]

SOURCES = [
    "WHO",
    "CDC",
    "World Bank",
    "UNICEF",
    "Government Health Portal"
]

# =========================================================
# GENERATE ORDER DATA
# =========================================================

def generate_order():
    category = random.choice(list(PRODUCT_CATALOG.keys()))
    product = random.choice(PRODUCT_CATALOG[category])

    random_days = random.randint(0, 365)
    order_dt = datetime.now() - timedelta(days=random_days)

    quantity = random.randint(1, 10)
    price = round(random.uniform(10, 2000), 2)

    return {
        "order_id": str(uuid.uuid4()),
        "customer_name": fake.name(),
        "product": product,
        "category": category,
        "quantity": quantity,
        "price": price,
        "city": fake.city(),
        "order_date": order_dt.strftime("%Y-%m-%d %H:%M:%S"),

        # Partition columns
        "year": order_dt.strftime("%Y"),
        "month": order_dt.strftime("%m"),
        "day": order_dt.strftime("%d")
    }

# =========================================================
# GENERATE COUNTRY INDICATOR DATA
# =========================================================

def generate_country_record():
    country_data = random.choice(COUNTRIES)

    random_days = random.randint(0, 180)
    record_date = datetime.now() - timedelta(days=random_days)

    daily_count = random.randint(0, 100000)
    rate_14_day = round(random.uniform(0.1, 500.0), 2)

    return {
        "country": country_data["country"],
        "country_code": country_data["country_code"],
        "continent": country_data["continent"],
        "population": country_data["population"],
        "indicator": random.choice(INDICATORS),
        "daily_count": daily_count,
        "date": record_date.strftime("%Y-%m-%d"),
        "rate_14_day": str(rate_14_day),
        "source": random.choice(SOURCES)
    }

# =========================================================
# CREATE DATAFRAMES
# =========================================================

orders_data = [generate_order() for _ in range(NUM_ORDER_RECORDS)]
country_data = [generate_country_record() for _ in range(NUM_COUNTRY_RECORDS)]

orders_df = pd.DataFrame(orders_data)
country_df = pd.DataFrame(country_data)

# =========================================================
# SHOW SAMPLE DATA
# =========================================================

print("\n========== ORDERS DATA ==========\n")
print(orders_df.head())

print("\n========== COUNTRY INDICATOR DATA ==========\n")
print(country_df.head())



S3_BUCKET = "orders123456sree"

ORDERS_S3_PREFIX = "sourceA/"
COUNTRY_S3_PREFIX = "sourceB/"

# Initialize S3 client
s3 = boto3.client("s3")


def upload_partitioned_orders(df):

    grouped = df.groupby(["year", "month", "day"])

    for (year, month, day), partition_df in grouped:

        # Unique CSV filename
        file_name = f"{uuid.uuid4()}.csv"

        s3_key = (
            f"{ORDERS_S3_PREFIX}"
            f"year={year}/"
            f"month={month}/"
            f"day={day}/"
            f"{file_name}"
        )

        # Convert dataframe to CSV in-memory
        csv_buffer = StringIO()

        final_df = partition_df.drop(
    columns=["year", "month", "day"]
            )

        final_df.to_csv(
            csv_buffer,
            index=False
        )

        # Upload using boto3 client
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=csv_buffer.getvalue()
        )

        print(f"Uploaded: s3://{S3_BUCKET}/{s3_key}")

# =========================================================
# UPLOAD COUNTRY CSV DIRECTLY
# =========================================================

def upload_country_dataset(df):

    file_name = f"{uuid.uuid4()}.csv"

    s3_key = f"{COUNTRY_S3_PREFIX}{file_name}"

    csv_buffer = StringIO()

    df.to_csv(
        csv_buffer,
        index=False
    )

    # Upload using boto3 client
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=csv_buffer.getvalue()
    )

    print(f"Uploaded: s3://{S3_BUCKET}/{s3_key}")

# =========================================================
# EXECUTE UPLOADS
# =========================================================

upload_partitioned_orders(orders_df)

upload_country_dataset(country_df)

print("\nUpload completed successfully!")