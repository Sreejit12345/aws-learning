# The lambda role must have AWSStepFunctionsFullAccess in order to send task success/failure to step functions.

import pandas as pd
import boto3
import io
import json

def lambda_handler(event, context):

    s3_client = boto3.client('s3')

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


        return data

    except Exception as e:
             
             data ={
            "schema_validation_result": None,
            "bucket": None,
            "key": None
            }
             return data
            

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