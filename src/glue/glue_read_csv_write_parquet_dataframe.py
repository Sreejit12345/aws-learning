'''

Note: This takes the default catalog created in the glue account

'''


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-glue-context.html#aws-glue-api-crawler-pyspark-extensions-glue-context-create-dataframe-from-catalog

# read directly from catalog and return a dataframe
# if we use useSparkDataSource= True, we can specify same options in additional_options like we do in spark.read  api.
# using useSparkDataSource directly gives a spark df, and not a glue dataframe

covid_df= glueContext.create_data_frame_from_catalog(
    database='dev-bronze-sales',
    table_name='sourceb',
    additional_options={'useSparkDataSource':True,'useCatalogSchema':True,'header':True}
    )
    

# read from raw location and returns dynamicframe
#https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame.html#aws-glue-api-crawler-pyspark-extensions-dynamic-frame-toDF
#https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame-reader.html#aws-glue-api-crawler-pyspark-extensions-dynamic-frame-reader-from_options
#https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-glue-context.html#aws-glue-api-crawler-pyspark-extensions-glue-context-create_dynamic_frame_from_options
#https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-csv-home.html

# if there are unresolved options, we must resolve before we convert to dataframe

orders_dynamic=glueContext.create_dynamic_frame_from_options(
    connection_type='s3',
    connection_options = {"paths": ["s3://orders123456sree/sourceA/"],"recurse":True},
    format='csv',
    format_options={
        "withHeader":True
    }
)

# convert to dataframe
orders_df= orders_dynamic.toDF()



#native spark read file
o_df=spark.read.format('csv').option('path',"s3://orders123456sree/sourceA/").option('header',True).option('inferSchema',True).load()



##########################################################################################################################

#write all 3 dataframes


# direct write to s3
# but this does not update the catalog by default,and we need to make sure crawler runs to register table and discover partitions.
o_df.write.format('parquet').option('path',"s3://orders123456sree/sourceA-op-sparknative/").mode('overwrite').save()


#save as table registers table and partitions in the glue catalog
covid_df.write.format('parquet').partitionBy('country').option('path',"s3://orders123456sree/sourceBopsparknative/").mode('overwrite').saveAsTable('`dev-bronze-sales`.`sourceBopsparknative`')



job.commit()