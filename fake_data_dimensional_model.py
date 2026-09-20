# This generates fake orders data(fact) and customers data (dimensions) and writes to s3 as partitioned datasets
# partition in s3 controlled by extract_date which is a non business date and simply tells when the extract job runs
# each day orders has incremental records new orders + exisitng orders (updates)
# customers is a full snapshot .

# orders schema

'''
order_id          STRING
customer_id       STRING
order_status      STRING
order_amount      DECIMAL(10,2)     
order_ts          TIMESTAMP
updated_ts        TIMESTAMP     #remember right now when updates happen for an order this field also gets updated which is not correct, we should make sure we preserve first value in silver or gold layer
extract_date      DATE
'''

# customers schema

'''
customer_id       STRING
customer_name     STRING
updated_ts        TIMESTAMP
extract_date      DATE
'''


import random
import datetime
import pandas as pd
import numpy as np
import boto3
from io import BytesIO
from faker import Faker

class S3Ops:

    def __init__(self):
        self.s3_client=boto3.client('s3')

    def put_obj(self,bucket,key,body):

        self.s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=body
                )
    

if(__name__=='__main__'):

    

    def generate_orders_for_one_partition(run_configs):
        # generates sample rows and dedups on order_id. This is the entire data for 1 partition

        cust_upper_limit=max(run_configs['customer_id_range'])
        extract_date=run_configs['extract_date']

        date_object = datetime.datetime.strptime(extract_date, "%Y-%m-%d")

        data= {
            'order_id':[ 'ORD-'+ str(i) for i in run_configs['order_id_range']],
            'customer_id':['CUST-'+ str(random.randint(1,cust_upper_limit)) for i in run_configs['order_id_range']],
            'order_status':[random.choice(['SUCCESS','FAILURE','REFUND','CANCELLED']) for i in run_configs['order_id_range']],
            'order_amount':[random.random() *100 for i in run_configs['order_id_range']],
            'order_ts':[datetime.datetime.now() for i in run_configs['order_id_range']],
            'updated_ts':[datetime.datetime.now() for i in run_configs['order_id_range']],
            'extract_date' : [date_object for i in run_configs['order_id_range']],
            }

        df=pd.DataFrame(data)

        df.drop_duplicates(subset=['order_id'], inplace=True) #since in 1 partition order_id will always be unique

        print(df)

        return df

    def generate_customers_for_one_partiiton(run_configs):
        # generates customers.

        extract_date=run_configs['extract_date']
        date_object = datetime.datetime.strptime(extract_date, "%Y-%m-%d")
        

        data={
        'customer_id' :['CUST-'+ str(i) for i in run_configs['customer_id_range']],
        'customer_name': [Faker().name() for i in run_configs['customer_id_range']],
        'updated_ts':[datetime.datetime.now() for i in run_configs['customer_id_range']],
        'extract_date' : [date_object for i in run_configs['customer_id_range']]
        }

        df=pd.DataFrame(data)
        
        df.drop_duplicates(subset=['customer_id'], inplace=True) #since customer_id will always be unique

        print(df)

        return df

    def split_df_into_multiple(df):
        # takes a big df and splits into 3 small dfs. This is to simulate multiple files in a single partition.

        df_arr=[]
        
        chunks = np.array_split(df, 3)  #splits into 3 dataframes
        for chunk in chunks:
            df_arr.append(chunk)

        return df_arr



    def write_parquet_files(df_arr,s3obj,bucket,dataset_type,run_configs):
        # Writes each chunk individually as parquet file, so no. elements in array = number of files created

        extract_date=run_configs['extract_date']

        key_prefix=f"{dataset_type}/extract_date={extract_date}/"

        for pos,i in enumerate(df_arr):
    
            key=key_prefix+ f'{dataset_type}-{pos}' +'.parquet'
            parquet_buffer = BytesIO()
            i.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy',index=False)
            s3obj.put_obj(bucket,key,parquet_buffer.getvalue())
            print(f'uploaded file {key}')




    def perform_extraction(load_type):
        # write orders data.
        # load type is a string : inital_load/second_load etc..

        s3obj=S3Ops()
        bucketname= "arshisreejitlove"

        run_configs=control_configs[load_type]

        odf=generate_orders_for_one_partition(run_configs)
        odf_arr= split_df_into_multiple(odf)
        write_parquet_files(odf_arr,s3obj,bucketname,"orders",run_configs)


        # write customers data
        
        cdf=generate_customers_for_one_partiiton(run_configs)
        cdf_arr= split_df_into_multiple(cdf)
        write_parquet_files(cdf_arr,s3obj,bucketname,"customers_snapshot",run_configs)


    

    #CONTROL CENRE:
    control_configs={
                        'inital_load': {'extract_date':'2026-09-19',
                                        'order_id_range':range(1,51),  #will generate orders from ORD-1 to ORD-50- with random customers  from 1-50
                                        'customer_id_range':range(1,51) #will generate all customers from CUST-1 to CUST 50
                                        },

                        'second_load': {'extract_date':'2026-09-20',
                                        'order_id_range':range(41,61),  #will generate orders from ORD-41 to ORD-60 with random customers from 1-60. Note orders 41 till 50 will be repeated and will count as updates
                                        'customer_id_range':range(1,61) #will generate all customers from CUST-1 to CUST 60
                                        },
                        'third_load':  {
                                        'extract_date': '2026-09-21',
                                        'order_id_range': range(51, 71),    # ORD-51 to ORD-70 (10 overlapping updates + 10 brand new orders)
                                        'customer_id_range': range(1, 71)   # CUST-1 to CUST-70 (Cumulative full snapshot of all customers to date)
                                        },
                        'fourth_load': {
                                        'extract_date': '2026-09-22',
                                        'order_id_range': range(60, 93),    # ORD-60 to ORD-92 (13 updates [60-72] + 20 new [73-92])
                                        'customer_id_range': range(1, 93)   # CUST-1 to CUST-92 (Full snapshot: 92)
                                        },

                        'fifth_load': {
                                        'extract_date': '2026-09-23',
                                        'order_id_range': range(88, 98),    # ORD-88 to ORD-97 (5 updates [88-92] + 5 new [93-97])
                                        'customer_id_range': range(1, 98)   # CUST-1 to CUST-97 (Full snapshot: 97)
                                        }

                    }


    #remember customer dataset is full load from source- so remember to just increase end  value of range in subsequent runs, start of range should be from 1 always.
    #Fact data is incrementls, but we have intentional overlaps, in orders because same order can up in multiple partitions due to returns/cancellations and updates
    #no hard deletes happen on either tables, only soft
    #initial load - 50 new orders and 50 new customers
    #second load -  10 overlapping orders(updates) and 10 new orders(inserts) / cust 1-50 gets updated and 51- 60 are new


    perform_extraction("inital_load")
    #perform_extraction("second_load")
    #perform_extraction("third_load")

        
        
