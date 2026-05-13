import json
import boto3
import pandas as pd
import uuid
from io import BytesIO
import datetime

'''
This lambda function gets triggered whenever a csv file is placed in a s3 bucket. This is the event on which the lambda
gets triggered. The csv is a order.csv with order information.

Post that- it use boto3 to download the csv in the local lambda function storage.
Then it reads the downloaded csv into a pandas dataframe, performs transformations and writes back to same bucket,
in a different location.


boto3 library is by default installed on aws lambda, but pandas is not. We can use layering approach , or we can zip
pandas in the zip and do a zip deployment.


zip deployemnt:

Make sure to be on a linux env, or windows(WSL) while packgaing dependencies. Aso, use the same python verison to avoid conflicts In WSL:

1) curl -LsSf https://astral.sh/uv/install.sh | sh              # download uv 
2) cd to src/lambda                                             # move to target directory
3) uv pip install   --target .   --python-platform x86_64-manylinux2014   --python 3.10   --only-binary :all:   "pandas<2.3.0"            #download pandas binaries in target directory
4) zip -r my_deployment_package.zip .                          # create a zip file, zipping all dependencies along with "lambda_function.py". The name of the zip can be anything, but the handler name should be "lambda_function.py" 


'''


def lambda_handler(event, context):

    file_name=str(uuid.uuid1()) + '.csv'
    op_key=f'reports/city_revenue_summary_{str(datetime.date.today())}.csv'
    
    try:

        bucket_name= event['Records'][0]['s3']['bucket']['name']
        key =event['Records'][0]['s3']['object']['key']

        s3_client=get_s3_client()
        download_file(s3_client,bucket_name,key,f'/tmp/{file_name}')
        df=read_local_csv_file(f'/tmp/{file_name}')
        final_df=perform_transformation(df)
        write_data_to_s3(s3_client,final_df,bucket_name,op_key)


    except Exception as e:
        print(f"Exception caught {e}")
    
    print('Finished processing!!!!!!')

    return {
        'statusCode': 200
    }

def get_s3_client():

    # as long as the role in aws lambda fucntion has access policy to S3 bucket , the config will work by default.

    s3_client = boto3.client('s3')
    return s3_client


def download_file(s3_client,bucket,key,filename):
    s3_client.download_file(bucket,key,filename)

def read_local_csv_file(path):
    df =pd.read_csv(path)
    return df

def perform_transformation(df):
    
    df['revenue']=df['quantity']*df['price']

    f_df=df.groupby('city').agg(
        total_revenue=('revenue','sum')
    ).reset_index()


    return f_df.sort_values(by=['total_revenue'],ascending=[False])

def write_data_to_s3(s3_client,df,bucket,key):

    csv_buffer = BytesIO()
    df.to_csv(csv_buffer,index=False)
    csv_buffer.seek(0)
    s3_client.upload_fileobj(csv_buffer,bucket,key)



