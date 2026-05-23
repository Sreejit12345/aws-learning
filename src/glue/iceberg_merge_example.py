#https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html
#https://apache.github.io/iceberg/docs/1.4.1/spark-writes/

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
    F.col('id')).withColumn(
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

spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.icebergDatabase")

all_tables=spark.sql("SHOW TABLES IN `icebergDatabase`").collect()
print(all_tables)
table_exists=False
for i in all_tables:
    if(i['namespace']=='icebergdatabase' and i['tableName']=='icebergmergeexample' and i['isTemporary']==False):
        table_exists=True
    

if(table_exists==False):
    
    logger.info("need to create table")
    
    
    # the table schema isn glue might be incorrectly shown , but all queries will run fine. 
    #https://repost.aws/questions/QUToXdoBgjTgiGMRFxcsY_3A/schema-incorrectly-showing-data-type-of-array-in-glue-catalog-when-using-delta-lake-table
    
    ## remember iceberg is inferred via the catalog, writeTo uses a catalog aware write- and since catalog is configured to iceberg the tabke is in iceberg
    df.writeTo(f"glue_catalog.icebergDatabase.icebergmergeexample") \
    .tableProperty("format-version", "2") \
    .create()
    
    logger.info("table created...")

else:
    logger.info("no need to create table- porceeding to merge")
    
    # register df dataframe -since iceberg enables sql only for merge 
    df.createOrReplaceTempView("source_table")
    
    spark.sql("""
    
    MERGE INTO glue_catalog.icebergDatabase.icebergmergeexample t   
    USING source_table s         
    ON t.random_int=s.random_int
    
    WHEN MATCHED THEN 
    UPDATE SET
    t.updated_ts = current_timestamp()
    
    WHEN NOT MATCHED THEN INSERT (
      random_int,
      current_ts,
      updated_ts
    )
    VALUES (
      s.random_int,
      s.current_ts,
      s.updated_ts
    )
    
    """)
    
     