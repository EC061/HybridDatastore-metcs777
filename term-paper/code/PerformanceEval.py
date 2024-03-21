import boto3
import psycopg2
from dotenv import load_dotenv
import os
import time
from prettytable import PrettyTable
import csv
import json

load_dotenv()
variable_names = [
    "DATABASE_USERNAME", "DATABASE_PASSWORD", "DATABASE_ENDPOINT",
    "DATABASE_NAME", "NOSQL_NAME", "AWS_REGION", "ACCESS_KEY",
    "SECRET_KEY", "SESSION_TOKEN"
]
assert all(os.getenv(var) for var in variable_names), "One or more environment variables are missing or empty"
env_vars = {var: os.getenv(var) for var in variable_names}
access_key = env_vars["ACCESS_KEY"]
secret_key = env_vars["SECRET_KEY"]
session_token = env_vars["SESSION_TOKEN"]
rds_user = env_vars["DATABASE_USERNAME"]
rds_pass = env_vars["DATABASE_PASSWORD"]
rds_endpoint = env_vars["DATABASE_ENDPOINT"]
rds_name = env_vars["DATABASE_NAME"]
dynamodb_name = env_vars["NOSQL_NAME"]
aws_region = env_vars["AWS_REGION"]
conn_string = f"dbname='{rds_name}' user='{rds_user}' host='{rds_endpoint}' password='{rds_pass}'"
address_fields = {'Street', 'City', 'State', 'PostalCode'}
local_sample_data = 'term-paper/data/customer_info_sample.csv'

# session = boto3.Session(
#     aws_access_key_id=access_key,
#     aws_secret_access_key=secret_key,
#     aws_session_token=session_token
# )

session = boto3.Session()
dynamodb = session.resource('dynamodb', region_name=aws_region)
table = dynamodb.Table(dynamodb_name)

def insert_into_dynamodb(customer_data):
    try:
        table.put_item(Item=customer_data)
    except Exception as e:
        print(f"Error inserting data into DynamoDB: {e}")

def get_from_dynamodb(customer_id, projection_expression):
    try:
        response = table.get_item(Key={'CustomerID': customer_id}, ProjectionExpression=projection_expression)
        return response

    except Exception as e:
        print(f"Error fetching data from DynamoDB: {e}")
        return None

def fetch_from_postgresql(customer_id):
    try:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        
        cur.execute(f"SELECT * FROM customer_info WHERE customerid = %s", (customer_id,))
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        if result:
            customer_data = {
                'CustomerID': result[0],
                'FirstName': result[1],
                'LastName': result[2],
                'Email': result[3],
                'PhoneNumber': result[4],
                'Address': {
                    'Street': result[5],
                    'City': result[6],
                    'State': result[7],
                    'PostalCode': result[8]
                },
                'DateOfBirth': result[9].strftime('%Y-%m-%d'),
                'AccountCreationDate': result[10].strftime('%Y-%m-%d'),
                'LastPurchaseDate': result[11].strftime('%Y-%m-%d'),
                'LoyaltyPoints': result[12]
            }
            return customer_data
        else:
            return None
    except Exception as e:
        print(f"Error fetching data from PostgreSQL: {e}")
        return None

def get_customer_data_fields(customer_id, fields):

    for i, field in enumerate(fields):
        if field in address_fields:
            fields[i] = f'Address.{field}'
    projection_expression = ", ".join(fields)
    
    try:
        response = get_from_dynamodb(customer_id, projection_expression)
        if 'Item' in response:
            # Handle the case where the loyalty points have changed in the PostgreSQL database but not in DynamoDB
            if 'LoyaltyPoints' in fields:
                customer_data = fetch_from_postgresql(customer_id)
                if customer_data:
                    if customer_data['LoyaltyPoints'] != response['Item']['LoyaltyPoints']:
                        response['Item']['LoyaltyPoints'] = customer_data['LoyaltyPoints']
                        insert_into_dynamodb({'CustomerID': customer_id, 'LoyaltyPoints': customer_data['LoyaltyPoints']})
                return response['Item']
        else:
            customer_data = fetch_from_postgresql(customer_id)
            if customer_data:
                insert_into_dynamodb(customer_data)
                return get_from_dynamodb(customer_id, projection_expression)['Item']
            else:
                return None
    except Exception as e:
        print(f"Error fetching customer data: {e}")
        return None

def get_customer_ids_from_csv(file_path, start, end):
    customer_ids = []
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for i, row in enumerate(reader):
            if i == 0:
                continue
            if i > end:
                break
            if i < start:
                continue
            customer_id = row[0]
            customer_ids.append(customer_id)
    return customer_ids


nosql_id = get_customer_ids_from_csv(local_sample_data, 1, 10)
fields_to_fetch = ['FirstName', 'LastName', 'Email', 'Street']
nosql_time_used = []
nosql_total_time = 0

for customer_id in nosql_id:
    start = time.time()
    customer_data = get_customer_data_fields(customer_id, fields_to_fetch)
    end = time.time()
    nosql_time_used.append(end - start)
    nosql_total_time += end - start

avg_nosql_time = sum(nosql_time_used) / len(nosql_time_used)
print(f"Average time used for fetching data from DynamoDB: {avg_nosql_time} seconds")
print(f"Total time used for fetching data from DynamoDB: {nosql_total_time} seconds")

hybrid_id = get_customer_ids_from_csv(local_sample_data, 11, 20)
fields_to_fetch = ['FirstName', 'LastName', 'Email', 'Street']
hybrid_time_used = []
hybrid_total_time = 0

for customer_id in hybrid_id:
    start = time.time()
    customer_data = get_customer_data_fields(customer_id, fields_to_fetch)
    end = time.time()
    hybrid_time_used.append(end - start)
    hybrid_total_time += end - start

    # Delete data from DynamoDB so the hybrid datastore can be tested again
    try:
        table.delete_item(Key={'CustomerID': customer_id})
    except Exception as e:
        print(f"Error deleting data from DynamoDB: {e}")

avg_hybrid_time = sum(hybrid_time_used) / len(hybrid_time_used)
print(f"Average time used for fetching data from the hybrid datastore: {avg_hybrid_time} seconds")
print(f"Total time used for fetching data from the hybrid datastore: {hybrid_total_time} seconds")

rds_id = get_customer_ids_from_csv(local_sample_data, 21, 30)
rds_time_used = []
rds_total_time = 0

for customer_id in rds_id:
    start = time.time()
    customer_data = fetch_from_postgresql(customer_id)
    end = time.time()
    rds_time_used.append(end - start)
    rds_total_time += end - start

avg_rds_time = sum(rds_time_used) / len(rds_time_used)
print(f"Average time used for fetching data from PostgreSQL: {avg_rds_time} seconds")
print(f"Total time used for fetching data from PostgreSQL: {rds_total_time} seconds")

# Example performance when accessing a sensitive field
fields_to_fetch = ['FirstName', 'LastName', 'Email', 'LoyaltyPoints']
sensitive_time_used = []
sensitive_total_time = 0

for customer_id in nosql_id:
    # alters the loyalty points of a customer in DynamoDB to force a difference between the two datastores
    altered_data = {
                'CustomerID': customer_id,
                'LoyaltyPoints': 10001
            }
    insert_into_dynamodb(altered_data)
    start = time.time()
    customer_data = get_customer_data_fields(customer_id, fields_to_fetch)
    end = time.time()
    sensitive_time_used.append(end - start)
    sensitive_total_time += end - start

avg_sensitive_time = sum(sensitive_time_used) / len(sensitive_time_used)
print(f"Average time used for fetching data from DynamoDB with a sensitive field: {avg_sensitive_time} seconds")
print(f"Total time used for fetching data from DynamoDB with a sensitive field: {sensitive_total_time} seconds")


# Save time used list and total time variable into a file
data = {
    "nosql": nosql_time_used,
    "hybrid": hybrid_time_used,
    "rds": rds_time_used,
    "sensitive": sensitive_time_used,
}

with open('term-paper/data/time_data.json', 'w') as file:
    json.dump(data, file)
 
# Create a table to display the average time used for fetching data from each datastore
table = PrettyTable()
table.field_names = ["Datastore", "Average Time: Per Request (seconds)", "Total Time: 10 Requests (seconds)"]
table.add_row(["Hybrid Datastore (best case)", avg_nosql_time, nosql_total_time])
table.add_row(["Hybrid Datastore (worst case)", avg_hybrid_time, hybrid_total_time])
table.add_row(["Hybrid Datastore (sensitive field)  ", avg_sensitive_time, sensitive_total_time])
table.add_row(["Relational Database (best case)", avg_rds_time, rds_total_time])
print(table)
 
