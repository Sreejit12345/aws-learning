from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.sql.types import StringType,IntegerType,StructField,StructType
import sys
from delta.tables import DeltaTable

conf= SparkConf()

conf.set(
    'spark.sql.extensions',
    'io.delta.sql.DeltaSparkSessionExtension'
)

conf.set(
    'spark.sql.catalog.spark_catalog',
    'org.apache.spark.sql.delta.catalog.DeltaCatalog'
)

conf.set(
    'spark.delta.logStore.class',
    'org.apache.spark.sql.delta.storage.S3SingleDriverLogStore'
)

spark=SparkSession.builder.appName("s").config(conf=conf).getOrCreate()


args=getResolvedOptions(sys.argv,
                        ['bucket',
                         'key'
                         ]
                         )

s3_path='s3://'+ args['bucket'] +'/' + args['key']


schema=StructType([
        StructField("name",StringType()),
        StructField("age",IntegerType()),
        StructField("email",StringType()),
        StructField("is_active",StringType())
]
        )

idf= spark.read.format("csv").option("header",True).schema(schema).\
    option("path",s3_path).load()


spark.sql("create database if not exists stepfunctions")

all_tables=spark.sql("SHOW TABLES IN stepfunctions").collect()

table_exists=False

for i in all_tables:
    if(i['namespace']=='stepfunctions' and i['tableName']=='stepfunctiontable' and i['isTemporary']==False):
        table_exists=True

if(table_exists==False):

    idf.write.format("delta").mode("append").\
        option('path',"s3://s3goldstepfucntionsdemo/stepfunctiontable/").\
        saveAsTable("stepfunctions.stepfunctiontable")

else:

    exisitng_df= DeltaTable.forName(spark,"stepfunctions.stepfunctiontable")

    exisitng_df.alias("e").merge(
    idf.alias("i"),
    'e.name = i.name') \
     .whenMatchedUpdate(set =
    {
      "age": "i.age",
      "email": "i.email",
      "is_active": "i.is_active",
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "name": "i.name",
      "age": "i.age",
      "email": "i.email",
      "is_active": "i.is_active",
    }
  ) \
  .execute()
    