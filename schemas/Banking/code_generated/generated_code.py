import os
import csv
from datetime import datetime, timedelta
from faker import Faker
import random

fake = Faker()

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_customers(num_rows):
    customers = []
    for _ in range(num_rows):
        dob = fake.date_of_birth(minimum_age=18, maximum_age=90)
        created_at = fake.date_time_this_decade()
        updated_at = fake.date_time_this_decade() if random.random() > 0.5 else None
        customer = {
            "CUSTOMER_ID": fake.unique.random_int(min=1, max=9999999999),
            "FIRST_NAME": fake.first_name(),
            "LAST_NAME": fake.last_name(),
            "DATE_OF_BIRTH": dob,
            "EMAIL": fake.unique.email(),
            "PHONE": fake.phone_number(),
            "STREET_ADDRESS": fake.street_address(),
            "CITY": fake.city(),
            "STATE_CODE": fake.state_abbr(),
            "POSTAL_CODE": fake.zipcode(),
            "KYC_STATUS": random.choices(['VERIFIED', 'PENDING', 'REJECTED'], weights=[80, 15, 5])[0],
            "RISK_SCORE": round(random.uniform(0, 100), 2),
            "CREATED_AT": created_at,
            "UPDATED_AT": updated_at
        }
        customers.append(customer)
    return customers

def generate_accounts(customers, num_rows):
    accounts = []
    for _ in range(num_rows):
        customer_id = random.choice(customers)["CUSTOMER_ID"]
        opened_date = fake.date_this_decade()
        closed_date = fake.date_this_decade() if random.random() > 0.85 else None
        balance = round(random.uniform(100, 10000), 2)
        overdraft_limit = round(random.uniform(0, 5000), 2)
        available_balance = round(random.uniform(0, balance + overdraft_limit), 2)
        account = {
            "ACCOUNT_ID": fake.unique.random_int(min=1, max=9999999999),
            "CUSTOMER_ID": customer_id,
            "ACCOUNT_NUMBER": fake.unique.bban(),
            "ACCOUNT_TYPE": random.choices(['CHECKING', 'SAVINGS'], weights=[70, 30])[0],
            "CURRENCY_CODE": 'USD',
            "BALANCE": balance,
            "AVAILABLE_BALANCE": available_balance,
            "STATUS": random.choices(['ACTIVE', 'DORMANT', 'CLOSED'], weights=[85, 10, 5])[0],
            "OPENED_DATE": opened_date,
            "CLOSED_DATE": closed_date,
            "BRANCH_CODE": fake.bban(),
            "OVERDRAFT_LIMIT": overdraft_limit,
            "INTEREST_RATE": round(random.uniform(0.01, 0.05), 3),
            "CREATED_AT": fake.date_time_this_decade(),
            "UPDATED_AT": fake.date_time_this_decade() if random.random() > 0.5 else None
        }
        accounts.append(account)
    return accounts

def generate_transactions(accounts, num_rows):
    transactions = []
    for _ in range(num_rows):
        account_id = random.choice(accounts)["ACCOUNT_ID"]
        transaction_ts = fake.date_time_this_decade()
        value_date = transaction_ts + timedelta(days=random.randint(0, 2))
        amount = round(random.expovariate(1/100), 2)  # Right-skewed distribution
        is_suspicious = random.random() < 0.02
        transaction = {
            "TRANSACTION_ID": fake.unique.random_int(min=1, max=9999999999),
            "ACCOUNT_ID": account_id,
            "TRANSACTION_TS": transaction_ts,
            "VALUE_DATE": value_date,
            "TRANSACTION_TYPE": random.choice(['DEBIT', 'CREDIT', 'TRANSFER', 'FEE', 'INTEREST']),
            "CHANNEL": random.choice(['ATM', 'ONLINE', 'BRANCH', 'MOBILE']),
            "AMOUNT": amount,
            "CURRENCY_CODE": 'USD',
            "DESCRIPTION": fake.sentence(),
            "MERCHANT_NAME": fake.company(),
            "MERCHANT_CATEGORY": fake.word(),
            "COUNTERPARTY_ACCOUNT": fake.bban(),
            "STATUS": random.choice(['COMPLETED', 'PENDING', 'FAILED']),
            "IS_SUSPICIOUS": is_suspicious,
            "REFERENCE_CODE": fake.unique.uuid4()
        }
        transactions.append(transaction)
    return transactions

def generate_loans(customers, accounts, num_rows):
    loans = []
    for _ in range(num_rows):
        customer = random.choice(customers)
        customer_id = customer["CUSTOMER_ID"]
        account_id = random.choice(accounts)["ACCOUNT_ID"] if random.random() > 0.5 else None
        principal_amount = round(random.uniform(1000, 50000), 2)
        outstanding_amount = round(random.uniform(0, principal_amount), 2)
        term_months = random.randint(6, 60)
        start_date = fake.date_this_decade()
        maturity_date = start_date + timedelta(days=term_months * 30)
        loan = {
            "LOAN_ID": fake.unique.random_int(min=1, max=9999999999),
            "CUSTOMER_ID": customer_id,
            "ACCOUNT_ID": account_id,
            "LOAN_TYPE": random.choice(['PERSONAL', 'HOME', 'AUTO', 'SME']),
            "PRINCIPAL_AMOUNT": principal_amount,
            "OUTSTANDING_AMOUNT": outstanding_amount,
            "INTEREST_RATE": round(random.uniform(0.01, 0.15), 3),
            "TERM_MONTHS": term_months,
            "START_DATE": start_date,
            "MATURITY_DATE": maturity_date,
            "STATUS": random.choice(['ACTIVE', 'PAID', 'DEFAULT']),
            "COLLATERAL_TYPE": fake.word(),
            "CREDIT_SCORE": random.randint(300, 850),
            "CREATED_AT": fake.date_time_this_decade(),
            "UPDATED_AT": fake.date_time_this_decade() if random.random() > 0.5 else None
        }
        loans.append(loan)
    return loans

def generate_loan_payments(loans, accounts, num_rows):
    payments = []
    for _ in range(num_rows):
        loan = random.choice(loans)
        account_id = random.choice(accounts)["ACCOUNT_ID"]
        payment_ts = fake.date_time_this_decade()
        due_date = payment_ts + timedelta(days=random.randint(1, 30))
        principal_component = round(random.uniform(0, loan["OUTSTANDING_AMOUNT"]), 2)
        interest_component = round(principal_component * loan["INTEREST_RATE"], 2)
        penalty_component = round(random.uniform(0, 100), 2)
        payment_amount = principal_component + interest_component + penalty_component
        payment_status = random.choice(['PAID', 'PENDING', 'FAILED', 'LATE'])
        paid_date = payment_ts if payment_status == 'PAID' else None
        payment = {
            "PAYMENT_ID": fake.unique.random_int(min=1, max=9999999999),
            "LOAN_ID": loan["LOAN_ID"],
            "ACCOUNT_ID": account_id,
            "PAYMENT_TS": payment_ts,
            "DUE_DATE": due_date,
            "PAID_DATE": paid_date,
            "PAYMENT_AMOUNT": payment_amount,
            "PRINCIPAL_COMPONENT": principal_component,
            "INTEREST_COMPONENT": interest_component,
            "PENALTY_COMPONENT": penalty_component,
            "PAYMENT_METHOD": random.choice(['BANK_TRANSFER', 'CREDIT_CARD', 'DEBIT_CARD']),
            "PAYMENT_STATUS": payment_status,
            "RECEIPT_NUMBER": fake.unique.uuid4(),
            "CREATED_AT": fake.date_time_this_decade()
        }
        payments.append(payment)
    return payments

def write_to_csv(filename, data):
    with open(os.path.join(OUTPUT_DIR, filename), mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

def main():
    customers = generate_customers(12000)
    write_to_csv('customers.csv', customers)

    accounts = generate_accounts(customers, 13000)
    write_to_csv('accounts.csv', accounts)

    transactions = generate_transactions(accounts, 15000)
    write_to_csv('transactions.csv', transactions)

    loans = generate_loans(customers, accounts, 10500)
    write_to_csv('loans.csv', loans)

    payments = generate_loan_payments(loans, accounts, 11000)
    write_to_csv('loan_payments.csv', payments)

if __name__ == "__main__":
    main()