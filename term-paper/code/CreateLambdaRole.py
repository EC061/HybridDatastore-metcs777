import boto3
import json
from dotenv import load_dotenv, set_key
import os

# Load environment variables from a .env file and assert that they are all set
load_dotenv()
variable_names = ["ACCESS_KEY", "SECRET_KEY", "SESSION_TOKEN", "LAMBDA_NAME"]
assert all(os.getenv(var) for var in variable_names), "One or more environment variables are missing or empty"
env_vars = {var: os.getenv(var) for var in variable_names}

access_key = env_vars["ACCESS_KEY"]
secret_key = env_vars["SECRET_KEY"]
session_token = env_vars["SESSION_TOKEN"]
lambda_name = env_vars["LAMBDA_NAME"]

session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    aws_session_token=session_token
)


iam_client = session.client('iam')
# This is the role that the Lambda function will assume when it is invoked
# Do not do this in production, use least possible privilege instead
assume_role_policy_document = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}

try:
    role = iam_client.get_role(RoleName=lambda_name)
    print(f"Role '{lambda_name}' already exists. ARN: {role['Role']['Arn']}")
except iam_client.exceptions.NoSuchEntityException:
    print(f"Role '{lambda_name}' not found. Creating new role.")
    try:
        create_response = iam_client.create_role(
            RoleName=lambda_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy_document),
            Description="Lambda execution role created by boto3",
        )
        role_arn = create_response['Role']['Arn']
        set_key('.env', 'LAMBDA_ARN', role_arn)
        policies = [
            'arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess',
            'arn:aws:iam::aws:policy/AmazonRDSFullAccess',
            'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        ]
        for policy_arn in policies:
            try:
                response = iam_client.attach_role_policy(
                    RoleName=lambda_name,
                    PolicyArn=policy_arn
                )
                print(f"Successfully attached policy {policy_arn} to role {lambda_name}.")
            except Exception as e:
                print(f"Error attaching policy {policy_arn} to role {lambda_name}: {str(e)}")
        
        print(f"Role '{lambda_name}' created successfully. ARN: {role_arn}")
    except Exception as e:
        print(f"Error creating role: {str(e)}")
