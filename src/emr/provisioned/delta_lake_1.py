# We are going to read delta tables from glue catalog and then write to glue catalog
# If EMR is deployed on a public subnet (rote to IG exists)- the EMR service role must have glueServiceRole attached for this to work
# remeber to enable "use glue catalog fo spark" during cluster creation.

# I am using emr 7.14.0- so delta jar is already pre-installed . https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-app-versions-7.x.html

# Remeber to add this to spark submit- so delta sepcifc jars are loaded

# If you use Amazon EMR releases 7.0.0 or higher, use /usr/share/aws/delta/lib/delta-spark.jar instead of /usr/share/aws/delta/lib/delta-core.jar.


# remeber this is under the default aws glue catalog created with name as accountId.
# # spark_catalog has a pointer to the default AWS account ID glue catalog, so all databases and tables get populated under that.


#paste below line to use delta specific jars.
'''
--conf “spark.jars=/usr/share/aws/delta/lib/delta-spark.jar,
     /usr/share/aws/delta/lib/delta-storage.jar,
     /usr/share/aws/delta/lib/delta-storage-s3-dynamodb.jar"
'''


'''
Sample spark submit:

spark-submit --deploy-mode client --conf spark.jars=/usr/share/aws/delta/lib/delta-spark.jar,/usr/share/aws/delta/lib/delta-storage.jar,/usr/share/aws/delta/lib/delta-storage-s3-dynamodb.jar s3://sreejits-bucket/spark-code/delta.py
'''

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


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

conf.set("spark.sql.catalogImplementation", "hive")
conf.set(
    'spark.hadoop.hive.metastore.client.factory.class',
    'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
)



spark=SparkSession.builder.appName("a").config(conf=conf).getOrCreate()


df= spark.sql("select * from `spark_catalog`.`dev-bronze-sales`.deltaexample")


df.write.format('delta').mode('overwrite').option("path","s3://sreejits-bucket/delta-op-emr").save()

