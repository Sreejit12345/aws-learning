import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-glue-context.html#aws-glue-api-crawler-pyspark-extensions-glue-context-create_dynamic_frame_from_catalog

#read dynamic frame from glue catalog
src_frame=glueContext.create_dynamic_frame_from_catalog(database='arshia', 
table_name='bkmrk_orders',
transformation_ctx="sreejits-ctx"
)

#convert dynamicframe to dataframe
df=src_frame.toDF()
df.show(20,False)

#perform transformations
df.printSchema()
df=df.withColumn('file_name',F.input_file_name())

#convert dataframe to dynamicframe
op_frame=DynamicFrame.fromDF(dataframe=df, glue_ctx=glueContext)


# The default behavior is to append here.
sink = glueContext.getSink(
    connection_type="s3", 
    path="s3://sreejitsgluebookmarkdemo/output/orders/",
    updateBehavior="UPDATE_IN_DATABASE",
    enableUpdateCatalog=True,
    compression="snappy",
    )

sink.setFormat("parquet", useGlueParquetWriter=True)
sink.setCatalogInfo(catalogDatabase='arshia', catalogTableName='bkmrk_orders_op')
sink.writeFrame(op_frame)



job.commit()