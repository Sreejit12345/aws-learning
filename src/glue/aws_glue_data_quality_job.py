# https://docs.aws.amazon.com/glue/latest/dg/dqdl.html?  


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

data = [
    (1, "C101", "PAID", 250.50, "2026-05-01"),
    (2, "C102", "SHIPPED", 99.99, "2026-05-02"),
    (3, None, "NEW", 10.00, "2026-05-03"),
    (4, "C104", "INVALID", -5.00, "2026-05-04"),
    (4, "C104", "PAID", 50.00, None)
]

columns = [
    "order_id",
    "customer_id",
    "status",
    "amount",
    "order_date"
]

df = spark.createDataFrame(data, columns)

ruleset='''
Rules = [
IsComplete "customer_id",
IsPrimaryKey  "order_id",
ColumnValues "amount" > 0,
ColumnValues "status" in ["NEW","PAID","SHIPPED"],
RowCount > 0,
ColumnExists "order_date"
]
'''

dqResults = EvaluateDataQuality.apply(
    frame=df,
    ruleset=ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "sreejit-dq",     #namespace under which dq rules are published
        "enableDataQualityCloudWatchMetrics": "true",
        "enableDataQualityResultsPublishing": "true",
    "resultsS3Prefix": "s3://my-bucket/orders123456sree/dq/"   #stores dq results here partitoned by year/month/day
    },
)



df.show() 
dqResults.show(truncate=False)


'''
Provides op schema like:  1 row per rule

root
 |-- Rule: string
 |-- Outcome: string
 |-- FailureReason: string (nullable)
 |-- EvaluatedMetrics: map<string, double>
 |-- EvaluatedRule: string

 
 '''

job.commit()