# scp this file to EMR cluster and run as spark submit
# spark-submit --deploy-mode client --master yarn basic.py
# spark-submit --deploy-mode cluster --master yarn basic.py



from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("basic").getOrCreate()

ip_df=spark.read.format("csv").option("path","s3://sreejits-bucket/employees/sample_recs.csv").option("inferSchema",True).load()

ip_df.write.format('parquet').option("path","s3://sreejits-bucket/employees_emr_op").save()