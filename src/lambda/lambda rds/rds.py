# pip install mysql-connector-python

import mysql.connector
import boto3
import os

os.environ["LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN"] = "1"


IAM_profile="database-admin"   #This is an IAM profile configured, in aws cli config file.
DBUserName='admin'  #has all privileges 

dev_session=boto3.Session(profile_name=IAM_profile)

auth_token = dev_session.client('rds', region_name='ap-south-1').\
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

    cursor.execute("select * from  sreejit12345.employees;")

    for i in cursor.fetchall():
        print(i)

finally:
      if(conn):
           conn.close()