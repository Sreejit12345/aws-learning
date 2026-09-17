import json
import boto3
import time

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

def lambda_handler(event, context):

    client=get_redshift_api()
    all_db=list_databases(client)

    print(f"all databases in cluster {all_db}")
    
    # creates sales table in the `dev`` database(defined by connection) in the `public`` schema

    create_table_sql="""
    create table if not exists public.sales 
    (
    salesid integer not null, 
    listid integer not null, 
    sellerid integer not null, 
    buyerid integer not null,
    pricepaid decimal(8,2), 
    saletime timestamp
    )
     """

    insert_table_sql="""
    insert into public.sales values (1000,1000,1000,1000,1000.00,'2022-01-01 00:00:00')
    """

    query_status=run_sql(client, [create_table_sql, insert_table_sql])

    print(f"query status {query_status}")

    return 1
    

def get_redshift_api():
    client = boto3.client('redshift-data')
    return client

def list_databases(client):
    response = client.list_databases(
        Database='dev',
        WorkgroupName='arn:aws:redshift-serverless:us-east-1:468094684048:workgroup/5b126c29-1203-473e-acdb-4ccd4898e7f8'
    )

    all_databases=response['Databases']

    # If next token exists, keep querying and appending results

    while(response.get('NextToken')!=None):

        next_n=response['NextToken']

        response = client.list_databases(
        Database='dev',
        WorkgroupName='arn:aws:redshift-serverless:us-east-1:468094684048:workgroup/5b126c29-1203-473e-acdb-4ccd4898e7f8',
        NextToken=next_n
        )

        temp_databases=response['Databases']

        all_databases.extend(temp_databases)

    print("all databases in workgroup", all_databases)

    return all_databases

def run_sql(client,sql_queries):
    
    # this runs async so we need to check back the status

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

