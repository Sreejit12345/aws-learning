import random
import datetime
import json
import boto3

'''

Function to generate mock data in this format:

{
"sensor_id": "SENS-WH01-T003",
"warehouse_id": "WH-01",
"zone": "cold-storage-A",
"reading_type": "temperature",
"value": -18.5,
"unit": "celsius",
"timestamp": "2026-03-16T10:30:00Z",
"battery_pct": 82
}


This also sends the message to SQS queue. Remeber to give AmazonSNSFullAccess to the IAM role of the lambda


'''


def lambda_handler(event, context):
    
    x=generate_mock()

    sqs_client=get_sqs_client()
    
    success_ct=0
    failed_ct=0

    for i in x:

        print(i)
        print('\n')
        print('\n')

        try:
            sqs_client.send_message(QueueUrl='https://sqs.ap-south-1.amazonaws.com/468094684048/coldchain-sensor-raw-queue', MessageBody=json.dumps(i))
            success_ct=success_ct+1

        except Exception as e:
            failed_ct=failed_ct+1
            print(e)

    total_messages_published=success_ct

    return {
        'statusCode': 200,
        'body': json.dumps({'messages_published':total_messages_published,
                            'messages_failed':failed_ct})
    }


def generate_mock():

    warehouse_list=['WH-01','WH-02','WH-03']
    zone_list=['cold-storage-A','cold-storage-B','dry-storage','loading-dock']
    reading_type=['temperature','humidity']
    
    num_readings=random.randint(3,5)

    reading_list=[]

    for i in range(0,num_readings):


        warehouse_id=random.choice(warehouse_list)
        zone=random.choice(zone_list)
        reading_type_op=random.choice(reading_type)

        if(reading_type_op=='temperature' and zone.startswith('cold-storage')==True):

            if(random.random()>0.15):      
                value=random.uniform(-25,-15)  #normal_cold_storage_temp
            
            else:  #induce 15% anomaly chance
                value=random.uniform(-14,2)  #anomaly cold storage temp

            unit='celsius'
            sensor_id=f'SENS-{warehouse_id}-T00{i}'

        elif(reading_type_op=='temperature' and zone.startswith('cold-storage')==False):

            if(random.random()>0.15):
                value=random.uniform(15,25)   #normal_dry_loading_dock_temp

            else:   #induce 15% anomaly chance
                
                value=random.uniform(0,10)  #anomaly dry storage temp

            unit='celsius'
            sensor_id=f'SENS-{warehouse_id}-T00{i}'

        elif(reading_type_op=='humidity'):

            if(random.random()>0.10):
                value=random.uniform(30,60)   #normal_humidity_pct

            else:  #induce 10% anomaly chance
                value=random.uniform(61,88)   #anomaly_humidity_pct

            unit='percent'
            sensor_id=f'SENS-{warehouse_id}-H00{i}'
        

        timestamp=datetime.datetime.now().isoformat()

        battery_pct=random.randrange(5,100)


        op={
            'sensor_id':sensor_id,
            'warehouse_id':warehouse_id,
            'zone':zone,
            'reading_type':reading_type_op,
            'value':value,
            'unit':unit,
            'timestamp':timestamp,
            'battery_pct':battery_pct
        }

        reading_list.append(op)
       
    return reading_list


def get_sqs_client():
    c= boto3.client('sqs')
    return c