import boto3
from dotenv import load_dotenv
import os
from botocore.exceptions import ClientError

load_dotenv()
variable_names = ["ACCESS_KEY", "SECRET_KEY", "SESSION_TOKEN",
                  "DATABASE_IDENTIFIER", "NOSQL_NAME", "AWS_REGION"]
assert all(os.getenv(var) for var in variable_names), "One or more environment variables are missing or empty"
env_vars = {var: os.getenv(var) for var in variable_names}
access_key = env_vars["ACCESS_KEY"]
secret_key = env_vars["SECRET_KEY"]
session_token = env_vars["SESSION_TOKEN"]
dynamodb_name = env_vars["NOSQL_NAME"]
rds_identifier = env_vars["DATABASE_IDENTIFIER"]
aws_region = env_vars["AWS_REGION"]

session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    aws_session_token=session_token
)

# Delete the RDS PostgreSQL instance and DynamoDB table used for the project
# Delete the DynamoDB table
dynamodb = session.resource('dynamodb', region_name=aws_region)
try:
    print(f"Deleting table '{dynamodb_name}'")
    table = dynamodb.Table(dynamodb_name)
    table.delete()

    # Wait until the table is deleted
    table.meta.client.get_waiter('table_not_exists').wait(TableName=dynamodb_name)
    print("Table deleted successfully.")
except ClientError as e:
    print(f"Failed to delete table: {e}")

# Delete the RDS PostgreSQL instance
rds_client = session.client('rds', region_name=aws_region)
try:
    response = rds_client.delete_db_instance(
        DBInstanceIdentifier=rds_identifier,
        SkipFinalSnapshot=True,
        DeleteAutomatedBackups=True,
    )
    print("RDS instance deletion initiated. This may take a few minutes.")
except Exception as e:
    print("Error deleting RDS instance:", e)

waiter = rds_client.get_waiter('db_instance_deleted')

try:
    waiter.wait(DBInstanceIdentifier=rds_identifier)
    print("RDS instance has been deleted.")
except Exception as e:
    print("Error waiting for RDS instance to be deleted:", e)
    exit()