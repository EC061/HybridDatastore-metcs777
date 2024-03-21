import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
import csv

load_dotenv()
variable_names = ["ACCESS_KEY", "SECRET_KEY", "SESSION_TOKEN", "NOSQL_NAME", "AWS_REGION"]

assert all(os.getenv(var) for var in variable_names), "One or more environment variables are missing or empty"
env_vars = {var: os.getenv(var) for var in variable_names}
access_key = env_vars["ACCESS_KEY"]
secret_key = env_vars["SECRET_KEY"]
session_token = env_vars["SESSION_TOKEN"]
dynamodb_name = env_vars["NOSQL_NAME"]
aws_region = env_vars["AWS_REGION"]
local_sample_data = 'term-paper/data/customer_info_sample.csv'

session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    aws_session_token=session_token
)

dynamodb = session.resource('dynamodb', region_name=aws_region)
table = dynamodb.Table(dynamodb_name)


# Inserts first 1000 records from the sample data into the DynamoDB table
sample_data = []

with open(local_sample_data, 'r') as file:
    reader = csv.reader(file)
    for i, row in enumerate(reader):
        if i == 0:
            continue
        if i > 1000:
            break
        item = {
            'CustomerID': row[0],
            'FirstName': row[1],
            'LastName': row[2],
            'Email': row[3],
            'PhoneNumber': row[4],
            'Address': {
                'Street': row[5],
                'City': row[6],
                'State': row[7],
                'PostalCode': row[8]
            },
            'DateOfBirth': row[9],
            'AccountCreationDate': row[10],
            'LastPurchaseDate': row[11],
            'LoyaltyPoints': int(row[12])
        }
        sample_data.append(item)  

for item in sample_data:
    try:
        table.put_item(Item=item)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ConditionalCheckFailedException':
            print(f"Conditional check failed for item {item['CustomerID']}.")
        else:
            print(f"Failed to insert item {item['CustomerID']}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred for item {item['CustomerID']}: {e}")
print(f'Data loading process completed. Total of {len(sample_data)} items loaded into the table.')