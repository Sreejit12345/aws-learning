#https://docs.aws.amazon.com/boto3/latest/

import boto3
from io import BytesIO,StringIO

def list_buckets(s3_client):
    # we can also use list_buckets(**kwargs) directly.
    paginator=s3_client.get_paginator('list_buckets')
    page_iterator=paginator.paginate()
    for page in page_iterator:
        for i in page['Buckets']:
            print(i['Name'])


def list_objects(bucket_name,s3_client):

    resp=s3_client.list_objects_v2(
    Bucket=bucket_name)
    
    for j in resp['Contents']:
        if(j['Size']>0):   #print files
            print(j['Key'])

def upload_file(file_path,s3_client,bucket,key):
    s3_client.upload_file(file_path,bucket,key)

def put_obj(bucket,key,body):
    # This also supports putting file like object like StringIO and bytesIO. Write to StringIO/BytesIO then use getValue
    # no need of local file

    s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=body
                )
    



def download_file(s3_client,bucket,key,filename):
    s3_client.download_file(bucket,key,filename)

def read_file(s3_client,bucket,key):
    resp=s3_client.get_object(Bucket=bucket,Key=key)

    print(resp['Body'].read())

def delete_object(s3_client,bucket,key):
    s3_client.delete_object(Bucket=bucket,Key=key)


if(__name__=='__main__'):

    s3_client = boto3.client('s3')    #creds are obtained through  ~/.aws/credentials file which can further be set by aws configure.

    list_buckets(s3_client)
    list_objects('sreejits-bucket',s3_client)
    #upload_file(r'C:\\Users\\SREEJIT\\Downloads\\PHOTO-2023-10-26-22-04-06.jpg',s3_client,'sreejits-bucket',"random/upload/input/abc.jpg")  # puts in localtion same as the key - works even if key doesn not exist

    random_string= "I am uploading this file randoomly to s3"
    output_stream = StringIO()
    output_stream.write(random_string)
    put_obj('sreejits-bucket','random/upload/input/random_string.txt',output_stream.getvalue())
    output_stream.close()


    #download_file(s3_client,'sreejits-bucket',"random/upload/input/abc.jpg",r"C:\\Users\\SREEJIT\\OneDrive\\Pictures\\sreejit_d.jpg")
    #read_file(s3_client,"sreejits-bucket",'raw_data/Day+8+-+Scripts.txt')
    #delete_object(s3_client,"sreejits-bucket","random/upload/input/abc.jpg")

