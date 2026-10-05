# The lambda role must have AWSStepFunctionsFullAccess in order to send task success/failure to step functions.

import pandas as pd
import boto3
import io
import json

def lambda_handler(event, context):

    s3_client = boto3.client('s3')
    sfn_client = boto3.client('stepfunctions')

    bucket= event['original_ip']['bucket']
    key= event['original_ip']['key']
    resp=s3_client.get_object(Bucket=bucket,Key=key)

    df = pd.read_csv(io.BytesIO(resp['Body'].read()), nrows=10)

    schema_validation= validate_schema(df,EXPECTED_COLS)

    try:
        data ={
            "schema_validation_result": schema_validation,
            "bucket": bucket,
            "key": key
        }

        sfn_client.send_task_success(
            taskToken=event.get('MyTaskToken'),
            output=json.dumps(data)
            )

    except Exception as e:
             
             sfn_client.send_task_failure(
                  taskToken=event.get('MyTaskToken'),
                    error="SomeError",
                    cause=str(e)
                    )
    


EXPECTED_COLS = {
    "name": "str",
    "age": "int",
    "email": "str",
    "is_active": "str"
}


def validate_schema(df: pd.DataFrame, expected_schema: dict) -> bool:
    missing = set(expected_schema.keys()) - set(df.columns)
    if missing:
        return False

    return True