# create a table bucket named `emr-iceberg-s3-bucket`. We see a `s3tablescatalog` federated created in aws glue catalog.
# under that we see a sub-catalog `emr-iceberg-s3-bucket`. Under that we see `databases` , under that `tables`.
# add this permission AmazonS3TablesFullAccess to the role

#https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-emr.html


#this code works eactly same as provisioned EMR. In fact all other code present under provisioned works exactly the same.
# just there is no need to explicityly mention icebeg defaults or iceberg JARS in EMR serverless.

# just remember- if we run this serverless EMR app in a default VPC - it wont have access to S3tables
# either run in a VPC with a public subnet- or a private subnet with s3tables endpoints along with other endpoints/ NAT gateway.


# s3tablesbucket.sreejits3managed.check23   -- refer to table, remember  s3tablesbucket catalog points to s3 table bucket in spark conf

# remember this uses a Amazon S3 Tables Catalog  to connect to s3Tables. If needed we can also use REST catalog.

'''

aws emr-serverless create-application \
  --name sreejit-app \
  --release-label emr-spark-8.0.0 \
  --type Spark \
  --network-configuration '{
    "subnetIds": ["subnet-0e16660224c185450", "subnet-0050fe20b601fd62f"],
    "securityGroupIds": ["sg-07f39c27787d1853c"]
  }' \
  --maximum-capacity '{
    "cpu": "100 vCPU",
    "memory": "100 GB",
    "disk": "100 GB"
  }' --region ap-south-1

'''

'''
aws emr-serverless start-job-run \
  --application-id "00g983cjejt5dm1t" \
  --execution-role-arn "arn:aws:iam::468094684048:role/sreejit-emr-serverless" \
  --name "my-spark-job" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://sreejits-bucket/spark-code/s3-tables-iceberg.py", 
      "sparkSubmitParameters": "--conf spark.dynamicAllocation.enabled=false --conf spark.driver.cores=1 --conf spark.executor.cores=1 --conf spark.executor.memory=4g --conf spark.executor.instances=1 --conf spark.driver.memory=4g --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"

'''

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

conf = SparkConf()

conf.set(
    'spark.sql.extensions',
    'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'
)
conf.set(
    'spark.sql.catalog.s3tablesbucket',
    'org.apache.iceberg.spark.SparkCatalog'
)
conf.set(
    'spark.sql.catalog.s3tablesbucket.catalog-impl',
    'software.amazon.s3tables.iceberg.S3TablesCatalog'
)
conf.set(
    'spark.sql.catalog.s3tablesbucket.warehouse',
    'arn:aws:s3tables:ap-south-1:468094684048:bucket/emr-iceberg-s3-bucket'  #change this to the arn of the s3 bucket `emr-iceberg-s3-bucket1`
)
conf.set(
    'spark.sql.defaultCatalog',
    's3tablesbucket'
)
conf.set
("spark.sql.catalog.s3tablesbucket.client.region",
 "ap-south-1")


spark=SparkSession.builder.appName("basic").config(conf=conf).getOrCreate()

spark.sql("create database if not exists s3tablesbucket.sreejits3managed") # creates under emr-iceberg-s3-bucket- as thats the warehouse set


spark.sql("""
    CREATE TABLE IF NOT EXISTS s3tablesbucket.sreejits3managed.check23 (
        Name STRING,
        Age INT
    )
    USING iceberg
""")

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]


df = spark.createDataFrame(data, schema=columns)

df.writeTo("s3tablesbucket.sreejits3managed.check23").createOrReplace()