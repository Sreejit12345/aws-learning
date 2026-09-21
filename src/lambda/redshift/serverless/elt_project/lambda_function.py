import json
import boto3
import time
import datetime
from sql_code import * # this import sql code

# https://docs.aws.amazon.com/boto3/latest/reference/services/redshift-data.html
# https://docs.aws.amazon.com/boto3/latest/reference/services/redshift-data/client/describe_statement.html
# https://docs.aws.amazon.com/boto3/latest/reference/services/redshift-data/client/batch_execute_statement.html

# Run this in lambda
# this is to connect to the serverless redshift cluster. 
# The IAM role attached to this lambda function will be used as the database user.  This database user should be created in redshift with admin account with proper grants beforehand
# Make sure the IAM role in this lambda = database user
# is used to query the cluster. This will make sure all objects will be created and queried with this database user.
# The database user should have grant access on the database.

# Do alter default privileges for user <this iam role/user>, so that future objects created under this account can be queried by the reqd people

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
# Also we pass a variable run_date, "run_date", in the lambda trigger event.
# This is the date of the batch pipeline run, for run_date x we read x-1 partitions from s3, so we must make sure this is available, do this by running producer script.
# so for run_date=2026-09-20, we read from s3 partitions 2026-09-19. This is the date of the data in s3. The run_date is passed as an env variable to the lambda function.

def lambda_handler(event, context):

    env_run_date=event['run_date']  # this is the date of the run, we will read from s3 partition of run_date-1

    query_status_copy_stg=copy_to_staging(env_run_date) #copy from s3 partition to staging tables, trucnate load

    if(query_status_copy_stg=="FINISHED"):

        print("Copy from s3 to staging finished .....")
    
        query_status_stg_main=stage_to_main()
    
        return query_status_stg_main


    return query_status_copy_stg
    

def get_redshift_api():
    client = boto3.client('redshift-data')
    return client


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

def copy_to_staging(run_date):

    parsed_run_date = datetime.datetime.strptime(run_date, "%Y-%m-%d").date()
    read_date = (parsed_run_date - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    client=get_redshift_api()

    # creates staging tables in the `dev`` database(defined by connection) in the `public`` schema

    query_status=run_sql(client, 
                         [  create_orders_stg_table_sql,
                            create_customers_stg_table_sql,
                            truncate_orders_stg_table_sql,
                            truncate_customers_stg_table_sql,
                            copy_orders_stg_table_sql.format(run_date=read_date),
                            copy_customers_stg_table_sql.format(run_date=read_date)
                        ]
                        )

    return query_status

def stage_to_main():

    client=get_redshift_api()

   

    execute_scd1="CALL scd1_upsert();"
    execute_scd2="CALL scd2();"


    query_status=run_sql(client, 
                            [  create_orders_table_sql,
                                create_customers_table_sql,
                                create_scd1_sp_sql,
                                create_scd2_sql,
                                execute_scd2,
                                execute_scd1
                            ]
                            )

    return query_status
