import json
import boto3
import pandas as pd
import io
from io import BytesIO
import datetime
import json



def lambda_handler(event, context):

    bucket='durable-func-test'
    key='durable_output1/op.csv'

    boto_client=boto3.client('lambda')

    file_content=event['file_variable']
    df = pd.read_csv(io.StringIO(file_content))

    
    ''''
    Now assume we are doing pandas df transformations.....  and then writing to a s3
    
    '''

    # audit dataframe and pass these to orcehstrator

    audit_dict={}

    audit_dict["row_count"] = len(df)
    audit_dict["column_count"] = len(df.columns)
    audit_dict["processing_time"] = datetime.datetime.now().isoformat()
    audit_dict["op_file_bucket"] = bucket
    audit_dict["op_file_key"] = key

    try:

        # make sure lambda has policy to write to s3.
        s3_client=get_s3_client()
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer,index=False)
        csv_buffer.seek(0)
        s3_client.upload_fileobj(csv_buffer,bucket,key)

        # make sure that this lambda has policy AWSLambda_FullAccess to pass callback to other lambda

        boto_client.send_durable_execution_callback_success(
        CallbackId=event['callback_id'],
        Result=json.dumps(audit_dict)
        )
    
    except Exception as e:

        error={
            'ErrorMessage':str(e)
        }

        # make sure that this lambda has policy AWSLambda_FullAccess to pass callback to other lambda

        boto_client.send_durable_execution_callback_failure(
        CallbackId=event['callback_id'],
        Error=error
)


    return {
        'statusCode': 200,
    }


def get_s3_client():

    # as long as the role in aws lambda fucntion has access policy to S3 bucket , the config will work by default.

    s3_client = boto3.client('s3')
    return s3_client