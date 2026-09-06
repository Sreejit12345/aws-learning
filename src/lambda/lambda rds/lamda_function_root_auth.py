# TO run this we need rds mysql deployed on private subnets and have its security group have an inbound rule to allow access from labmda SG on port 3306
# We will use root user and db password auth here- ideally we should use IAM auth as shown here: "src\lambda\lambda rds\lambda_function.py"


# for vpc/subnet/SG provisioning:  src\cloudformation\rds_networking_private_subnets.yml and src\cloudformation\rds pvtsubnet.yml
# for lambda provisioning src\cloudformation\lambda_rds_pvt_subnet.yml

# pip install mysql-connector-python      packkaged by zip deployment
# The clouforamtion for lambda just creates a lambda , we need this code along with mysql-connector-python zipped and deployed via console.


# # THis code simply truncates stg table and appends data from s3 event.
# create an orders file in s3 and create a trigger to trigger whenever a csv files lands in folder.


import mysql.connector
import boto3
import os
import json

def lambda_handler(event, context):
  

    try:
        conn = mysql.connector.connect(
                host="sreejit1234.cf0qu4c0i6i8.ap-south-1.rds.amazonaws.com",
                database="sreejit12345",
                user="sreejit",
                password="sreejit123#s2d3",
                autocommit=True
        
        )

        cursor= conn.cursor()

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
