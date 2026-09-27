# We are going to read delta tables from glue catalog and then write to glue catalog
# If EMR is deployed on a public subnet (rote to IG exists)- the EMR service role must have glueServiceRole attached for this to work
# remeber to enable "use glue catalog fo spark" during cluster creation.

# I am using emr 7.14.0- so delta jar is already pre-installed . https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-app-versions-7.x.html

# https://docs.aws.amazon.com/emr/latest/ReleaseGuide/Deltacluster-spark-glue.html

# In the "software settings" tab paste this string:

"""
[{"Classification":"delta-defaults", "Properties":{"delta.enabled":"true"}}, {"Classification":"spark-hive-site", "Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}] - how to do this in console
"""
# now the delta jars are loaded automatically- we dont need to mention jars as conf.

# remeber this is under the default aws glue catalog created with name as accountId.
# # spark_catalog has a pointer to the default AWS account ID glue catalog, so all databases and tables get populated under that.


from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


conf = SparkConf()

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


spark=SparkSession.builder.appName("a").config(conf=conf).getOrCreate()


df= spark.sql("select * from `spark_catalog`.`dev-bronze-sales`.deltaexample")


df.write.format('delta').mode('overwrite').option("path","s3://sreejits-bucket/delta-op-emr1234").save()

