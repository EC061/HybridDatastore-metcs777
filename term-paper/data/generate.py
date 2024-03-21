import csv
import random
from datetime import datetime, timedelta

def str_time_prop(start, end, format, prop):
    stime = datetime.strptime(start, format)
    etime = datetime.strptime(end, format)
    ptime = stime + prop * (etime - stime)
    return ptime.strftime(format)

def random_date(start, end, prop):
    return str_time_prop(start, end, '%Y-%m-%d', prop)

def generate_sample_data(num_records, file_path):
    # Ensure that customer IDs and emails are unique
    used_customer_ids = set()
    used_emails = set()
    
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["CustomerID", "FirstName", "LastName", "Email", "PhoneNumber", "Street", "City", "State", "PostalCode", "DateOfBirth", "AccountCreationDate", "LastPurchaseDate", "LoyaltyPoints"])
        
        while len(used_customer_ids) < num_records:
            customer_id = f"CID{random.randint(10000, 99999)}"
            if customer_id in used_customer_ids:
                continue
            
            first_name = f"First{random.randint(1, 100)}"
            last_name = f"Last{random.randint(1, 100)}"
            email = f"{first_name.lower()}_{last_name.lower()}@example.com"
            
            if email in used_emails:
                continue  
            
            phone_number = f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            street = f"{random.randint(1, 9999)} Main St"
            city = "City" + str(random.randint(1, 100))
            state = "State" + str(random.randint(1, 50))
            postal_code = f"{random.randint(10000, 99999)}"
            date_of_birth = random_date("1950-01-01", "2010-12-31", random.random())
            account_creation_date = random_date("2015-01-01", "2023-01-01", random.random())
            last_purchase_date = random_date("2023-01-01", "2024-01-01", random.random())
            loyalty_points = random.randint(0, 10000)
            
            writer.writerow([customer_id, first_name, last_name, email, phone_number, street, city, state, postal_code, date_of_birth, account_creation_date, last_purchase_date, loyalty_points])
            
            used_customer_ids.add(customer_id)
            used_emails.add(email)

sample_file_path = 'term-paper/data/customer_info_sample.csv'
generate_sample_data(3000, sample_file_path)
