#https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html

'''
create a key named --conf and put the value as:


   spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions  --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog  --conf spark.sql.catalog.glue_catalog.warehouse=s3://iceberg-demo123/warehouse/  --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog  --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true


Under the warehouse, it has a folder for db then table whuch has /data and /metadata




We also need to create one more parameter where we mention the datalake format, follow docs

'''



# remeber unlike delta we cannot override the default spark_catalog to use iceberg tables, so aws has a pre configured "glue_catalog"
# provisioned for us, which can support iceberg. 
# Internally the glue_catalog has a pointer to the default AWS account ID glue catalog, so all databases and tables get populated under that.

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SparkConf
from pyspark.sql import functions as F


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)

logger = glueContext.get_logger()

spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# generate 2 col df with random_int and current_timestamp

df = spark.range(1, 1000).withColumn(
    "random_int",
    (F.rand() * 100).cast("int")
).withColumn(
    "current_ts",
    F.current_timestamp()
).withColumn(
    "updated_ts",
    F.current_timestamp()
)

df=df.dropDuplicates(['random_int']).drop('id')



'''
Prerequisites: You will need to provision a catalog for the Iceberg library to use. When using the AWS Glue Data Catalog, AWS Glue makes this straightforward. The AWS Glue Data Catalog is pre-configured for
use by the Spark libraries as glue_catalog. Data Catalog tables are identified by a databaseName and a tableName
'''
# we are creating a database here. Note the glue catalog is already pre-configured to be used


# do note the database "icebergDatabase" also shows up under the default catalog in AWS glue catalog UI. Internally spark job uses the  glue_catalog instead of spark_catalog.
#
spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.icebergDatabase")

df.writeTo(f"glue_catalog.icebergDatabase.icebergexample") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

logger.info("table loaded!")
job.commit()