from aws_durable_execution_sdk_python.config import Duration,CallbackConfig
from aws_durable_execution_sdk_python.context import DurableContext, StepContext, durable_step
from aws_durable_execution_sdk_python.execution import durable_execution
import boto3
import json

'''

To attach a trigger

# make sure to publish a version and mention full ARN while configuring trigger from s3 portal.

EG ARN:
#arn:aws:lambda:ap-south-1:468094684048:function:durablelambdaOrchs:1


Ref: https://docs.aws.amazon.com/lambda/latest/dg/durable-invoking.html


'''

@durable_step
# This step receives raw event and extracts the bucket name key and returns a tuple
def parse_event_return_csv_path(step_context: StepContext, event) -> str:
    step_context.logger.info("Hello from 'parse_event_return_csv_path' step")

    bucket_name= event['Records'][0]['s3']['bucket']['name']
    key =event['Records'][0]['s3']['object']['key']

    return (bucket_name,key)

@durable_step
# This step receives tuple of bucket_name,key and downloads file into local dirctory and returns the content of file as string
def read_file_and_save_local(step_context: StepContext, s3_tuple) -> str:
    step_context.logger.info("Hello from 'read_file_and_save_local' step")

    file_name=s3_tuple[1].split('/')[-1]
    file_path=f'/tmp/{file_name}'

    s3_client=get_s3_client()

    s3_client.download_file(s3_tuple[0],s3_tuple[1],file_path)

    with open(file_path,'r') as f:
        x=f.read()
       
    return x    #this is okay fro small files, for big files this will crash and its better to store in s3.


@durable_execution
def lambda_handler(event, context) -> dict:

    s3_tuple = context.step(parse_event_return_csv_path(event))  #step1

    file_variable=context.step(read_file_and_save_local(s3_tuple))  #step2

    context.wait(Duration.from_seconds(10))    #pause executing without wasting lambda time

    context.logger.info("Waited for 10 seconds without consuming CPU.")

    config = CallbackConfig(
        timeout=Duration.from_seconds(360),
    )

    callback = context.create_callback(name="wait-for-ext-lambda",config=config)   #create a callback
    invoke_ext_lambda(callback.callback_id, file_variable)

    
    # Execution suspends here until the external system calls back.

    result = callback.result()

    

    return {
        "statusCode": 200
    }


def get_s3_client():

    # as long as the role in aws lambda fucntion has access policy to S3 bucket , the config will work by default.

    s3_client = boto3.client('s3')
    return s3_client

def get_lambda_client():
    lambda_client = boto3.client('lambda')
    return lambda_client


def download_file(s3_client,bucket,key,filename):
    s3_client.download_file(bucket,key,filename)

def invoke_ext_lambda(callback_id,file_variable):

    lambda_client=get_lambda_client()

    pay= {"callback_id":callback_id,
        "file_variable":file_variable

        }

        #AWSLambda_FullAccess  must be provided to the role of this lambda in order to invoke the other lambda.

    lambda_client.invoke(
        FunctionName="durablelambdaWorker",
        InvocationType ='RequestResponse',
        Payload=json.dumps(pay)
    

    )

