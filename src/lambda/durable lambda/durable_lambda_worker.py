import json
import boto3

def lambda_handler(event, context):
    
    print(event)

    client=boto3.client('lambda')

    # We must make sure the role of this lambda has access to the one its sending callback to..
    #AWSLambda_FullAccess  must be provided to the role of this lambda

    response = client.send_durable_execution_callback_success(
    CallbackId=event['callback_id'],
    Result=b'bytes'
)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
