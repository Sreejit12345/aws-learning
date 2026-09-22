# Pre Req we need to first create a glue connection to redshift. The test connection should work
# make sure the glue connection is used in the glue job in advanced configuration.
# this glue job runs inside the subnet and vpc where redshift runs, also it injects ENI inside subnet
# We also need s3 access- basically glue redshift connections needs s3 accesss for logging and temp files
# for private subnet- either NAT gateway or VPC endpoint for s3 is needed.
# for public subnet- internet access is needed for s3 access


# the tables get created if they dont exist.



from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark=SparkSession.builder.appName('s').getOrCreate()

spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

idf=spark.read.format('parquet').option('path',"s3://arshisreejitlove/customers_snapshot/extract_date=2026-09-19/").load()

# write using password credentials - the table owner is the database user based on pwd and user.

idf.write.format('jdbc').\
    option('url','jdbc:redshift://default-workgroup.468094684048.us-east-1.redshift-serverless.amazonaws.com:5439/dev').\
    option('dbtable','public.customers_snapshot_glue').\
    option('user','admin').\
    option('password','NewStart#s2d3s2d3').\
    mode('overwrite').\
    save()


# https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-redshift-auth.html
# https://docs.aws.amazon.com/redshift/latest/mgmt/redshift-spark-connector-authentication.html

# write using iam roles 
# note the iam in the jdbc url
# the temp directory is used for intermediate data storage.
# the aws_iam role is used to read and write data to redshift. This role should have proper grants on the redshift database objects.

# the database user is created automatically based on iam role. That would be the owner of the table created in redshift. The iam role should have proper grants on the database objects.



# We need STS and redshift serverless vpc endpoints created.
# the IAM role must be added to the redshift serverless workgroup.
# the DB user corresponding to the IAM role must be created in redshift with proper grants on the database objects.
# Add a tag RedshiftDataFullAccess to the serverless workgroup. This is because the defualt permission expects a tag on the workgroup to allow the IAM role to access the workgroup. This is a known issue with redshift serverless and iam roles. 
# The tag is needed for the IAM role to be able to access the workgroup and run queries on it.

idf.write.format('io.github.spark_redshift_community.spark.redshift').\
    option('url','jdbc:redshift:iam://default-workgroup.468094684048.us-east-1.redshift-serverless.amazonaws.com:5439/dev').\
    option('dbtable','public.customers_snapshot1_glue').\
    option("tempdir", "s3a://arshisreejitlove/temp").\
    option("aws_iam_role", "arn:aws:iam::468094684048:role/redshiftglue-crawler").\
    mode('overwrite').\
    save()


