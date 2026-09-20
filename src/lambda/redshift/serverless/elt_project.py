import json
import boto3
import time
import os

# https://docs.aws.amazon.com/boto3/latest/reference/services/redshift-data.html
# https://docs.aws.amazon.com/boto3/latest/reference/services/redshift-data/client/describe_statement.html
# https://docs.aws.amazon.com/boto3/latest/reference/services/redshift-data/client/batch_execute_statement.html

# Run this in lambda
# this is to connect to the serverless redshift cluster. 
# The IAM role attached to this lambda function will be used as the database user.  This database user should be created in redshift with admin account with proper grants beforehand
# Make sure the IAM role in this lambda = database user
# is used to query the cluster. This will make sure all objects will be created and queried with this database user.
# The database user should have grant access on the database.

# Do alter Defalut privileges for user <this iam role/user>, so that future objects created under this account can be queried by the reqd people

# The lambda IAM role needs to have the following permissions attached:

# AmazonRedshiftFullAccess
# AmazonRedshiftQueryEditorV2ReadWriteSharing  - so that any human user can assume this role and perform operations on behalf
# AWSLambdaBasicExecutionRole
# AmazonS3FullAccess
# AmazonRedshiftDataFullAccess

# trust policy:
# any aws lambda service
# any aws account

# Remember our source is coming from 'fake_data_dimensional_model.py'. THis is the data producer that produces parquet
#  

def lambda_handler(event, context):


    query_status=copy_to_staging() #copy from s3 partition to staging tables, trucnate load


    return query_status
    

def get_redshift_api():
    client = boto3.client('redshift-data')
    return client

def get_lambda_role_arn():
    # this function returns the arn of the role the lambda fucntion assumes

    sts_client=boto3.client("sts")
    response = sts_client.get_caller_identity()

    return response['Arn']


def run_sql(client,sql_queries):

    # remember the table is created by the database user corresponding to the iam role this lambda runs on
    
    # this runs async so we need to check back the status by polling.

    resp= client.batch_execute_statement(
        Sqls=sql_queries,
        Database='dev',
        #ExecutionMode= 'AUTO_COMMIT', #each sql is comitted individually and 1 sql failure does not impact other.
        WorkgroupName='arn:aws:redshift-serverless:us-east-1:468094684048:workgroup/5b126c29-1203-473e-acdb-4ccd4898e7f8'
    )

    sql_id=resp['Id']

    status=client.describe_statement(Id=sql_id)

    # finished means completed successfully
    while(status['Status'] not in ['ABORTED','FAILED','FINISHED']):
        time.sleep(5)
        print('sleeping .....')
        status=client.describe_statement(Id=sql_id)

    return status['Status']

def copy_to_staging():

    client=get_redshift_api()

    arn=get_lambda_role_arn()

    # creates staging tables in the `dev`` database(defined by connection) in the `public`` schema

    create_orders_stg_table_sql="""
    CREATE TABLE if not exists orders_staging (
        order_id      VARCHAR(64)     NOT NULL,
        customer_id   VARCHAR(64)     NOT NULL,
        order_status  VARCHAR(32)     NOT NULL,
        order_amount  NUMERIC(10, 2)  NOT NULL,
        order_ts      TIMESTAMP       NOT NULL,
        updated_ts    TIMESTAMP       NOT NULL,
        extract_date  DATE            NOT NULL
    )
    DISTSTYLE KEY
    DISTKEY (order_id)
     """

    create_customers_stg_table_sql="""
            CREATE TABLE if not exists customers_staging (
            customer_id   VARCHAR(64)     NOT NULL,
            customer_name VARCHAR(255)    NOT NULL,
            updated_ts    TIMESTAMP       NOT NULL,
            extract_date  DATE            NOT NULL
        )
        DISTSTYLE ALL
         """

    truncate_orders_stg_table_sql="truncate table dev.public.orders_staging"
    truncate_customers_stg_table_sql="truncate table dev.public.customers_staging"


    copy_orders_stg_table_sql=f"""
    COPY dev.public.orders_staging
    from s3://arshisreejitlove/orders/extract_date=2026-09-19/
    iam_role {arn}
    region us-east-1
    format as parquet;
    """

    copy_customers_stg_table_sql=f"""
    COPY dev.public.customers_staging
    from s3://arshisreejitlove/customers_snapshot/extract_date=2026-09-19/
    iam_role {arn}
    region us-east-1
    format as parquet;
    """

    query_status=run_sql(client, 
                         [  create_orders_stg_table_sql,
                            create_customers_stg_table_sql,
                            truncate_orders_stg_table_sql,
                            truncate_customers_stg_table_sql,
                            copy_orders_stg_table_sql,
                            copy_customers_stg_table_sql
                        ]
                        )


    return query_status

