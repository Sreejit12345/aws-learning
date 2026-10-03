# create an application and submit a batch job. We can also not specify subnet and SG- that way it gets deployed in default VPC aws managed


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


# launch a spark job
'''
aws emr-serverless start-job-run \
  --application-id "00g983cjejt5dm1t" \
  --execution-role-arn "arn:aws:iam::468094684048:role/sreejit-emr-serverless" \
  --name "my-spark-job" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://sreejits-bucket/spark-code/basic.py",
      "sparkSubmitParameters": "--conf spark.dynamicAllocation.enabled=false --conf spark.driver.cores=1 --conf spark.executor.cores=1 --conf spark.executor.memory=4g --conf spark.executor.instances=1 --conf spark.driver.memory=4g --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    }
  }'  --region ap-south-1

'''


from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("basic").getOrCreate()

ip_df=spark.read.format("csv").option("path","s3://sreejits-bucket/employees/sample_recs.csv").option("inferSchema",True).load()

ip_df.write.format('parquet').option("path","s3://sreejits-bucket/employees_emr_op").save()