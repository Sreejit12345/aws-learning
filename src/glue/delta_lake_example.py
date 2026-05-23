#https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-delta-lake.html

# remember to enable all properties as mentioned in documentation
# note in this example, we override the default spark_catalog with  additional properties for delta, internally the 
# spark_catalog has a pointer to the default AWS account ID glue catalog, so all databases and tables get populated under that.

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
from pyspark import SparkConf
from pyspark.sql import functions as F
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])


conf = SparkConf()


#we also enable -datalake-formats delta at the job level in glue, to make sure glue loads delta lake libraries.

# enable delta specific behavior and commands for spark
conf.set(
    'spark.sql.extensions',
    'io.delta.sql.DeltaSparkSessionExtension'
)

#replace sparks native catalog with delta aware behavior
conf.set(
    'spark.sql.catalog.spark_catalog',
    'org.apache.spark.sql.delta.catalog.DeltaCatalog'
)

#transaction safety
conf.set(
    'spark.delta.logStore.class',
    'org.apache.spark.sql.delta.storage.S3SingleDriverLogStore'
)

sc = SparkContext(conf=conf)

glueContext = GlueContext(sc)
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


all_tables=spark.sql("SHOW TABLES IN `dev-bronze-sales`").collect()

table_exists=False
for i in all_tables:
    if(i['namespace']=='`dev-bronze-sales`' and i['tableName']=='deltaexample' and i['isTemporary']==False):
        table_exists=True
    

if(table_exists==False):
    
    logger.info("need to create table")
    
    
    # the table schema isn glue might be incorrectly shown , but all queries will run fine. 
    #https://repost.aws/questions/QUToXdoBgjTgiGMRFxcsY_3A/schema-incorrectly-showing-data-type-of-array-in-glue-catalog-when-using-delta-lake-table
    
    df.write.format('delta').mode('append').option('path',"s3://orders123456sree/deltaexample/").saveAsTable('`dev-bronze-sales`.`deltaexample`')
    
    logger.info("table created...")

else:
    logger.info("no need to create table- porceeding to merge")
    
    deltaTable=DeltaTable.forPath(spark,'s3://orders123456sree/deltaexample/')
    
    deltaTable.alias('t').merge(
    df.alias('s'),
    "t.random_int=s.random_int").\
    whenMatchedUpdate(set ={"updated_ts":F.current_timestamp()}).\
    whenNotMatchedInsert(values =
    {
        "random_int":"s.random_int",
        "current_ts":"s.current_ts",
        "updated_ts":"s.updated_ts"
    }
    ).execute()
    
    
    
    

job.commit()