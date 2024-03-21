import boto3
from dotenv import load_dotenv, set_key
import os
import sys
from botocore.exceptions import ClientError
import time

# Load environment variables from a .env file and assert that they are all set
load_dotenv()
variable_names = [
    "DATABASE_USERNAME", "DATABASE_PASSWORD", "DATABASE_IDENTIFIER",
    "DATABASE_NAME", "ACCESS_KEY", "SECRET_KEY", "SESSION_TOKEN", "NOSQL_NAME", "AWS_REGION"
]
assert all(os.getenv(var) for var in variable_names), "One or more environment variables are missing or empty"
env_vars = {var: os.getenv(var) for var in variable_names}

rds_user = env_vars["DATABASE_USERNAME"]
rds_pass = env_vars["DATABASE_PASSWORD"]
rds_identifier = env_vars["DATABASE_IDENTIFIER"]
rds_name = env_vars["DATABASE_NAME"]
rds_class = 'db.t3.micro'
rds_engine = 'postgres'
rds_storage = 20
access_key = env_vars["ACCESS_KEY"]
secret_key = env_vars["SECRET_KEY"]
session_token = env_vars["SESSION_TOKEN"]
dynamodb_name = env_vars["NOSQL_NAME"]
aws_region = env_vars["AWS_REGION"]
# session = boto3.Session(
#     aws_access_key_id=access_key,
#     aws_secret_access_key=secret_key,
#     aws_session_token=session_token
# )

session = boto3.Session()
# No SQL Database: DynamoDB
start_time = time.time()
dynamodb = session.resource('dynamodb', region_name=aws_region)

try:
    print("Creating DynamoDB table...")
    existing_tables = session.client('dynamodb', region_name=aws_region).list_tables()['TableNames']
    if dynamodb_name in existing_tables:
        print("Table already exists!")
    else:
        table = dynamodb.create_table(
            TableName=dynamodb_name,
            KeySchema=[
                {
                    'AttributeName': 'CustomerID',
                    'KeyType': 'HASH'
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'CustomerID',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 100,
                'WriteCapacityUnits': 100
            }
        )
        table.meta.client.get_waiter('table_exists').wait(TableName=dynamodb_name)
        print("Table created successfully!")
except ClientError as e:
    print(e.response['Error']['Message'])
print(f"Time taken to create DynamoDB: {time.time() - start_time:.2f} seconds")

# Relational Database: AWS RDS
# Create a new RDS instance with PostgreSQL engine and t3.micro instance class
start_time = time.time()
rds_client = session.client('rds', region_name=aws_region)
try:
    response = rds_client.create_db_instance(
        DBInstanceIdentifier=rds_identifier,
        AllocatedStorage=rds_storage,
        DBInstanceClass=rds_class,
        Engine=rds_engine,
        MasterUsername=rds_user,
        MasterUserPassword=rds_pass,
        DBName=rds_name,
        BackupRetentionPeriod=0,
        PubliclyAccessible=True
    )
    print("RDS instance is being created. This may take a few minutes.")
except rds_client.exceptions.DBInstanceAlreadyExistsFault:
    print("An instance with this identifier already exists.")
except Exception as e:
    print("An error occurred:", e)
    sys.exit(1)

# Get the endpoint of the RDS instance after successful creation
waiter = rds_client.get_waiter('db_instance_available')
try:
    waiter.wait(DBInstanceIdentifier=rds_identifier)
    print("RDS instance is now available.")

    response = rds_client.describe_db_instances(DBInstanceIdentifier=rds_identifier)
    db_instances = response['DBInstances']
    if db_instances:
        db_instance = db_instances[0]
        endpoint = db_instance['Endpoint']['Address']
        print(f"Endpoint: {endpoint}")
        set_key('.env', 'DATABASE_ENDPOINT', endpoint)
except Exception as e:
    print("Error waiting for RDS instance to become available:", e)
    sys.exit(1)
print(f"Time taken to create RDS: {time.time() - start_time:.2f} seconds")

# Add current ip to AWS security group inbound rules
# This should not be used in production, it allows tcp traffic from any IP address to port 5432
start_time = time.time()
ec2_client = session.client('ec2', region_name=aws_region)
security_groups = ec2_client.describe_security_groups()['SecurityGroups']
default_sg_id = None
for sg in security_groups:
    if sg['GroupName'] == 'default':
        default_sg_id = sg['GroupId']
        break

if default_sg_id:
    print(f"Default Security Group ID: {default_sg_id}")
    ip_permission = {
        'IpProtocol': 'tcp',
        'FromPort': 5432,
        'ToPort': 5432,
        'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
    }
    
    try:
        ec2_client.authorize_security_group_ingress(
            GroupId=default_sg_id,
            IpPermissions=[ip_permission]
        )
        print("Inbound rule added to the default security group to allow PostgreSQL access")
    except ec2_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            print("Inbound rule for PostgreSQL access already exists in the default security group.")
        else:
            print(f"Error updating security group: {e}")
            sys.exit(1)
else:
    print("Default security group not found.")
    sys.exit(1)
print(f"Time taken to create security group: {time.time() - start_time:.2f} seconds")