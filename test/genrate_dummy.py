import mysql.connector
from faker import Faker
import random

# Connect to MySQL database
db_connection = mysql.connector.connect(
    host="148.113.6.50",
    port=3309,
    user="root",  # Replace with your actual database username
    password="root_password",  # Replace with your actual database password
)

cursor = db_connection.cursor()

# Instantiate Faker
fake = Faker()

# Create and populate the personal_info database
def create_and_populate_personal_info():
    cursor.execute("CREATE DATABASE IF NOT EXISTS personal_info_db")
    cursor.execute("USE personal_info_db")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS personal_info (
            id INT AUTO_INCREMENT PRIMARY KEY,
            full_name VARCHAR(100),
            email VARCHAR(100),
            phone_number VARCHAR(15),
            address VARCHAR(255),
            dob DATE
        )
    """)
    for _ in range(1000):
        full_name = fake.name()
        email = fake.email()
        phone_number = fake.msisdn()[:15]
        address = fake.address().replace("\n", " ")
        dob = fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d')
        cursor.execute("""
            INSERT INTO personal_info (full_name, email, phone_number, address, dob)
            VALUES (%s, %s, %s, %s, %s)
        """, (full_name, email, phone_number, address, dob))
    db_connection.commit()

# Create and populate the financial_info database
def create_and_populate_financial_info():
    cursor.execute("CREATE DATABASE IF NOT EXISTS financial_info_db")
    cursor.execute("USE financial_info_db")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS financial_info (
            id INT AUTO_INCREMENT PRIMARY KEY,
            account_number VARCHAR(20),
            ifsc_code VARCHAR(11),
            bank_name VARCHAR(100),
            balance DECIMAL(15, 2),
            transaction_date DATE
        )
    """)
    for _ in range(1000):
        account_number = fake.bban()
        ifsc_code = fake.swift()[:11]
        bank_name = fake.company()
        balance = fake.random_number(digits=7)
        transaction_date = fake.date_this_decade().strftime('%Y-%m-%d')
        cursor.execute("""
            INSERT INTO financial_info (account_number, ifsc_code, bank_name, balance, transaction_date)
            VALUES (%s, %s, %s, %s, %s)
        """, (account_number, ifsc_code, bank_name, balance, transaction_date))
    db_connection.commit()

# Create and populate the medical_info database
def create_and_populate_medical_info():
    cursor.execute("CREATE DATABASE IF NOT EXISTS medical_info_db")
    cursor.execute("USE medical_info_db")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS medical_info (
            id INT AUTO_INCREMENT PRIMARY KEY,
            patient_id VARCHAR(36),
            blood_group VARCHAR(5),
            allergies VARCHAR(255),
            medical_history TEXT,
            doctor_name VARCHAR(100)
        )
    """)
    for _ in range(1000):
        patient_id = fake.uuid4()
        blood_group = random.choice(['A+', 'A-', 'B+', 'B-', 'O+', 'O-', 'AB+', 'AB-'])
        allergies = fake.text(max_nb_chars=50)
        medical_history = fake.text(max_nb_chars=200)
        doctor_name = fake.name()
        cursor.execute("""
            INSERT INTO medical_info (patient_id, blood_group, allergies, medical_history, doctor_name)
            VALUES (%s, %s, %s, %s, %s)
        """, (patient_id, blood_group, allergies, medical_history, doctor_name))
    db_connection.commit()

# Create and populate the other_pii database
def create_and_populate_other_pii():
    cursor.execute("CREATE DATABASE IF NOT EXISTS other_pii_db")
    cursor.execute("USE other_pii_db")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS other_pii (
            id INT AUTO_INCREMENT PRIMARY KEY,
            aadhar_number VARCHAR(12),
            passport_number VARCHAR(10),
            voter_id VARCHAR(15),
            pan_number VARCHAR(10),
            driving_license_number VARCHAR(15)
        )
    """)
    for _ in range(1000):
        aadhar_number = str(fake.random_int(min=100000000000, max=999999999999))
        passport_number = fake.bothify(text="??######")
        voter_id = str(fake.random_int(min=1000000000, max=9999999999))
        pan_number = fake.bothify(text="????#####")
        driving_license_number = fake.bothify(text="DL##########")
        cursor.execute("""
            INSERT INTO other_pii (aadhar_number, passport_number, voter_id, pan_number, driving_license_number)
            VALUES (%s, %s, %s, %s, %s)
        """, (aadhar_number, passport_number, voter_id, pan_number, driving_license_number))
    db_connection.commit()

# Call functions to create and populate each database
create_and_populate_personal_info()
create_and_populate_financial_info()
create_and_populate_medical_info()
create_and_populate_other_pii()

# Close the connection
cursor.close()
db_connection.close()

print("Separate databases created and populated successfully.")
