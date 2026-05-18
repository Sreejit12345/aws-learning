'''
This lambda consumes events from data_generator_lambda.py file via the SQS queue.
This lambda also sends email notifications for anomalies via SNS. Remeber to give AmazonSNSFullAccess to IAM role.
THis lambda also writes data for all readings to an s3 bucket. Remember to give AmazonS3FullAccess to IAM Role.

This gets trigerred by SQS message. In this we demonstrate auto delete of messages from SQS queue post processing.
The "Report batch item failures" feature must be enabled from SQS trigger.

Remember the lamda IAM role must have AmazonSQSFullAccess role on the SQS queue

'''

#https://docs.aws.amazon.com/lambda/latest/dg/example_serverless_SQS_Lambda_batch_item_failures_section.html

import json
import os
import boto3
import io
import datetime

def lambda_handler(event, context):

    s3_cl=get_s3_client()
    
    #messages in batch_item_failures re-appear in queue , others get auto deleted
    batch_item_failures = []
    sqs_batch_response = {}
    binary_buffer = io.BytesIO()

    for message in event['Records']:
        
        try:
            
            msg_body = json.loads(message['body'])
            json_bytes = (json.dumps(msg_body) + "\n").encode('utf-8')       #converting dict to json
            binary_buffer.write(json_bytes)
            detect_anomalies(msg_body)  #this internaly takes cares of detection and notification of anomaly

            
        except Exception as e:
            print("failure  : ",message['messageId'])
            print("execption: ", e)
            batch_item_failures.append({"itemIdentifier": message['messageId']})


    binary_buffer.seek(0)

    s3_cl.upload_fileobj(
    Fileobj=binary_buffer,
    Bucket="coldchain-processed-data",
    Key = f"{datetime.datetime.now().date().year}/{datetime.datetime.now().date().month}/{datetime.datetime.now().date().day}/readings_{datetime.datetime.now()}.jsonl" 
)

    
    sqs_batch_response["batchItemFailures"] = batch_item_failures

    return sqs_batch_response


def detect_anomalies(msg_body):

    
    # parse env variables:

    battery_threshold_upper=int(os.getenv('battery_threshold_upper'))
    battery_threshold_lower=int(os.getenv('battery_threshold_lower'))
    humidity_threshold_lower=int(os.getenv('humidity_threshold_lower'))
    humidity_threshold_upper=int(os.getenv('humidity_threshold_upper'))
    temp_threshold_cold_lower=int(os.getenv('temp_threshold_cold_lower'))
    temp_threshold_cold_upper=int(os.getenv('temp_threshold_cold_upper'))
    temp_threshold_hot_lower=int(os.getenv('temp_threshold_hot_lower'))
    temp_threshold_hot_upper=int(os.getenv('temp_threshold_hot_upper'))

    

    # battery anomaly
    battery_pct=msg_body['battery_pct']


    if(battery_pct<battery_threshold_lower or battery_pct> battery_threshold_upper):
        notifiy_anomalies(type='battery anomaly',msg_body=msg_body)


    reading_type=msg_body['reading_type']
    val=msg_body['value']
    zone=msg_body['zone']


     #humidity anomaly

    if(reading_type=='humidity'):
        if(val<humidity_threshold_lower or val>humidity_threshold_upper):
            notifiy_anomalies(type='humidity anomaly',msg_body=msg_body)
    
    else:

        #temperature anomaly

        if(zone.startswith('cold-storage')==True):
            if(val<temp_threshold_cold_lower or val>temp_threshold_cold_upper):
                notifiy_anomalies(type='temperature anomaly cold',msg_body=msg_body)

        else:
             if(val<temp_threshold_hot_lower or val>temp_threshold_hot_upper):
                notifiy_anomalies(type='temperature anomaly warm',msg_body=msg_body)



def notifiy_anomalies(type,msg_body):

    sns_client= get_sns_client()

    sns_client.publish(
            TopicArn='arn:aws:sns:ap-south-1:468094684048:coldchain-warehouse-alerts',
            Message=f'Anomaly: {msg_body}',
            Subject=f'{type} anomaly detected'
        )


def get_s3_client():

    # as long as the role in aws lambda fucntion has access policy to S3 bucket , the config will work by default.

    s3_client = boto3.client('s3')
    return s3_client



def get_sns_client():
    sns_client = boto3.client('sns')
    return sns_client
