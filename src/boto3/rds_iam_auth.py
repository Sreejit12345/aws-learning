# TO run this we need rds mysql deployed on public subnets and have its security group have an inbound rule to connect from laptop wifi
# Also post database creation- login from admin master user and create 2 users 'admin' and 'developer'. Grant only select on 'developer', and 
# grant all on 'admin'.
#  We also need 2 policies attached, with action 'rds-db:connect'. dev user policy should only have access to do 'rds-db:connect' on the developer database user.
# admin user policy should only have access to do 'rds-db:connect' on the admin database user.

# we also configure access keys for both users and set up profile in config.

# essentially users/groups who have 'rds-db:connect' on that specific database user, will be able to raise token and authenticate to that db user




# for vpc/subnet/SG provisioning:  src\cloudformation\rds_networking_public_subnets.yml and src\cloudformation\rds.yml

# pip install mysql-connector-python



import mysql.connector
import boto3
import os

os.environ["LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN"] = "1"


IAM_profile="database-admin"   #This is an IAM profile configured, in aws cli config file.
# This has permission to raise token to database

DBUserName='admin'  #has all privileges 


'''
IAM_profile="database-developer"   #This is an IAM profile configured, in aws cli config file.
# This has permission to raise token to database

DBUserName='developer'  #has select privileges only
'''


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