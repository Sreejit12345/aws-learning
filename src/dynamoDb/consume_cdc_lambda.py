# https://aws-sdk-pandas.readthedocs.io/en/stable/index.html
# We use this AWS SDK for Pandas as it provides maximum libraries and helper methods for interactive with other AWS services

'''
This code gets triggered on a dynamoDB table stream, (OLD AND NEW IMAGE) and writes to a s3 partiitoned dataset.
This also creates a glue database if not exists and registers table + partition in the glue database.

This uses "AWS SDK for pandas" which is a helper library https://aws-sdk-pandas.readthedocs.io/en/stable/about.html

Pre-Req:

1) Run: src/dynamoDb/basic_ecommerce_table.py from local (with create_table_only) argument- this only creates a DynamoDb table with stream enabled

Create this lambda and enable trigger from above dynamoDB stream. Configure batch_size and batch_window as reqd.
Make sure the lambda role has reqd permission on dynamoDB and s3 and glue.

Have this lambda layer installed: AWSSDKPandas-Python314 [arn:aws:lambda:ap-south-1:336392948345:layer:AWSSDKPandas-Python314:12]
Increase lambda timeout to 15 mins and also make sure memory is increased to 2000Mb.

2) Run src/dynamoDb/basic_ecommerce_table.py from local without any argument. This runs the code to exeecute operations on dynamoDB.

3) The glue database "dynamocdc" and table "cdc_log" should be visible and ca nbe queries from aws glue.
'''

from boto3.dynamodb.types import TypeDeserializer
import pandas as pd
import awswrangler as wr


def lambda_handler(event, context):

    def dynamo_to_python(dynamo_obj):
        # takes a dynamoDB low-level json object and converts to python type
        deserializer = TypeDeserializer()
        deserialized_document = {k: deserializer.deserialize(v) for k, v in dynamo_obj.items()}
        return deserialized_document

    def create_db_if_not_exists():
        databases = wr.catalog.databases()
        if "dynamocdc" not in databases.values:
            wr.catalog.create_database("dynamocdc")
        else:
            print("Database dynamocdc already exists")

    converted_dict={}

    create_db_if_not_exists()

    for e in event.get('Records',[]):
        
        top_level=e.get('dynamodb')

        if(e.get('eventName')=='INSERT'):
            print("Insert event detected...")

            converted_obj=dynamo_to_python(top_level.get('NewImage'))
            converted_obj['delta_type']="INSERT"
            

        elif(e.get('eventName')=='MODIFY'):
            print("Update event detected...")
            converted_obj=dynamo_to_python(top_level.get('NewImage'))
            converted_obj['delta_type']="MODIFY"

        else:
            print('Delete event detected..')
            converted_obj=dynamo_to_python(top_level.get('OldImage'))
            converted_obj['delta_type']="REMOVE"

        converted_obj['ApproximateCreationDateTime']=top_level.get('ApproximateCreationDateTime')
        converted_obj['SequenceNumber']=top_level.get('SequenceNumber')

        for k in converted_obj.keys():
            if(k not in converted_dict):
                converted_dict[k]=[converted_obj[k]]
            else:
                converted_dict[k].append(converted_obj[k])


    print(converted_obj)
    df=pd.DataFrame(converted_dict)

    # for display: 
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_colwidth", None)
    pd.set_option("display.width", None)

    df["current_timestamp"] = pd.Timestamp.now(tz="UTC")
    df["hour_partition"] = df["current_timestamp"].dt.strftime("%Y-%m-%d-%H")

    print(df)

    path="s3://sreejits-bucket/dynamocdc"


    res=wr.s3.to_parquet(df=df, path=path, dataset=True, mode="append", partition_cols=["hour_partition"],database='dynamocdc',
                         table='cdc_log')

    print(res)

    