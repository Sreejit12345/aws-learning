import boto3

client = boto3.client('sts')

response = client.get_caller_identity() #Returns details about the IAM user or role whose credentials are used to call the operation.

print(response) 