# Create table then load sample data into db
import psycopg2
from psycopg2 import OperationalError, errors
from dotenv import load_dotenv
import os
import time

load_dotenv()
variable_names = [
    "DATABASE_USERNAME", "DATABASE_PASSWORD", "DATABASE_ENDPOINT",
    "DATABASE_NAME"
]
assert all(os.getenv(var) for var in variable_names), "One or more environment variables are missing or empty"
env_vars = {var: os.getenv(var) for var in variable_names}
rds_user = env_vars["DATABASE_USERNAME"]
rds_pass = env_vars["DATABASE_PASSWORD"]
rds_endpoint = env_vars["DATABASE_ENDPOINT"]
rds_name = env_vars["DATABASE_NAME"]
local_sample_data = 'term-paper/data/customer_info_sample.csv'

start_time = time.time()
try:
    conn_string = f"dbname='{rds_name}' user='{rds_user}' host='{rds_endpoint}' password='{rds_pass}'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS customer_info (
        CustomerID VARCHAR PRIMARY KEY,
        FirstName VARCHAR,
        LastName VARCHAR,
        Email VARCHAR UNIQUE,
        PhoneNumber VARCHAR,
        Street VARCHAR,
        City VARCHAR,
        State VARCHAR,
        PostalCode VARCHAR,
        DateOfBirth DATE,
        AccountCreationDate DATE,
        LastPurchaseDate DATE,
        LoyaltyPoints INT
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    print("Table 'customer_info' has been created.")

    # Delete all pre-existing rows in the table
    cursor.execute("DELETE FROM customer_info;")
    conn.commit()
    print("All pre-existing rows in 'customer_info' have been deleted.")

    # Load sample data into the table
    with open(local_sample_data, 'r') as f:
        next(f)  
        cursor.copy_from(f, 'customer_info', sep=',', columns=('customerid', 'firstname', 'lastname',
                                                       'email', 'phonenumber', 'street',
                                                       'city', 'state', 'postalcode',
                                                       'dateofbirth', 'accountcreationdate', 
                                                       'lastpurchasedate', 'loyaltypoints'))
    conn.commit()
    print("Sample data has been loaded into the 'customer_info' table.")

except OperationalError as e:
    print(f"A connection error occurred: {e}")
except errors.UniqueViolation as e:
    print(f"An error occurred within psycopg2: {e}")
    conn.rollback()
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
    print(f"Time taken to load data into RDS: {time.time() - start_time:.2f} seconds")