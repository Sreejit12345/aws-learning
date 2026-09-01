# TO run this we need rds mysql deployed on public subnets and have its security group have an inbound rule to connect from laptop wifi
# Also post database creation- login from admin master user and create 2 database users 'admin' and 'developer'. Grant only select on 'developer', and 
# grant all on 'admin' database user.
#  We also need 2 policies attached, with action 'rds-db:connect'. dev user policy should only have access to do 'rds-db:connect' on the developer database user.
# admin user policy should only have access to do 'rds-db:connect' on the admin database user.

#make sure that the IAM role of this lambda has the admin user policy attached.
#also lambda uses EC2 ip-ranges so make sure the ip cidr of EC2 in region 'ap-south-1' is whitelisted on rds sg for port 3306.
#https://docs.aws.amazon.com/vpc/latest/userguide/aws-ip-ranges.html


# for vpc/subnet/SG provisioning:  src\cloudformation\rds_networking_public_subnets.yml and src\cloudformation\rds.yml

# pip install mysql-connector-python      packkaged by zip deployment



# THis code simply truncates stg table and appends data from s3 event.

# create an orders file in s3 and create a trigger to trigger whenever a csv files lands in folder.



import mysql.connector
import boto3
import os
import json

def lambda_handler(event, context):
    os.environ["LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN"] = "1"

    # the iam role of lambda should have access to do rds-db:connect on admin db user

    DBUserName='admin'  #has all privileges 

    '''
    DBUserName='developer'  #has select privileges only
    '''

    auth_token = boto3.client('rds', region_name='ap-south-1').\
    generate_db_auth_token(DBHostname='sreejit1234.cf0qu4c0i6i8.ap-south-1.rds.amazonaws.com', 
                        Port=3306, DBUsername=DBUserName, Region='ap-south-1')

    conn=None

    try:
        conn = mysql.connector.connect(
                host="sreejit1234.cf0qu4c0i6i8.ap-south-1.rds.amazonaws.com",
                database="sreejit12345",
                user=DBUserName,
                password=auth_token,
                autocommit=True
        
        )

        cursor= conn.cursor()

        # GRANT ALL PRIVILEGES ON `%`.* TO 'admin'@'%';   This is reqd for admin to create DB and have all access on DB.
        cursor.execute("create database if not exists db1;")

        cursor.execute("""
        create table if not exists db1.employees_staging 
        (
            employee_id INT PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            department VARCHAR(50),
            hire_date DATE,
            salary DECIMAL(10, 2)
        );
        
        """)

        cursor.execute("""
                truncate table db1.employees_staging ;
                """)

        insert_query = "INSERT INTO db1.employees_staging (employee_id, first_name,last_name,department,hire_date,salary) VALUES (%s, %s, %s, %s, %s, %s)"
        


        bucket_name= event['Records'][0]['s3']['bucket']['name']
        key =event['Records'][0]['s3']['object']['key']
        
        s3_client=get_s3_client()

        resp= s3_client.get_object(Bucket=bucket_name,Key=key)

        lines=resp['Body'].readlines()  #each line is an array

        db_recs=[]   #needs to be an array of tuple in same order as insert
        for l in lines:

            ll=l.decode('utf-8').strip()
            line_arr=ll.split(',') #since i/p file is csv . Also assumeing no """", is there in csv and dates in yyyy-MM-dd format. 
            # And no header and emply line. In prod use csv module from python

            record = (
            int(line_arr[0].strip()),    # employee_id
            line_arr[1].strip(),         # first_name
            line_arr[2].strip(),         # last_name
            line_arr[3].strip(),         # department
            line_arr[4].strip(),         # hire_date (passes string straight to DB)
            float(line_arr[5].strip())   # salary
        )

            db_recs.append(record)


        cursor.executemany(insert_query, db_recs)




    finally:
        if(conn): 
            conn.close()
        
    return {
        "message": "Run Succeeded!"
        }

def get_s3_client():

    # as long as the role in aws lambda fucntion has access policy to S3 bucket , the config will work by default.

    s3_client = boto3.client('s3')
    return s3_client
