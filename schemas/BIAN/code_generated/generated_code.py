import os
import csv
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()


# __DLS_TEMPORAL_SAFE_PATCH__
from datetime import date, datetime, time

def _dls_to_date(v):
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return date.today()
        s = s.replace('Z', '+00:00')
        try:
            return date.fromisoformat(s[:10])
        except Exception:
            try:
                return datetime.fromisoformat(s).date()
            except Exception:
                return date.today()
    return date.today()

def _dls_to_datetime(v):
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime.combine(v, time.min)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return datetime.now()
        s = s.replace('Z', '+00:00')
        try:
            return datetime.fromisoformat(s)
        except Exception:
            try:
                return datetime.combine(date.fromisoformat(s[:10]), time.min)
            except Exception:
                return datetime.now()
    return datetime.now()

def _dls_safe_date_between(fake_obj, *args, **kwargs):
    start = _dls_to_date(kwargs.get('start_date', date(1970, 1, 1)))
    end = _dls_to_date(kwargs.get('end_date', date.today()))
    if start > end:
        start, end = end, start
    kwargs['start_date'] = start
    kwargs['end_date'] = end
    return getattr(fake_obj, 'date_between')(*args, **kwargs)

def _dls_safe_date_time_between(fake_obj, *args, **kwargs):
    start = _dls_to_datetime(kwargs.get('start_date', datetime(1970, 1, 1)))
    end = _dls_to_datetime(kwargs.get('end_date', datetime.now()))
    if start > end:
        start, end = end, start
    kwargs['start_date'] = start
    kwargs['end_date'] = end
    return getattr(fake_obj, 'date_time_between')(*args, **kwargs)

def _dls_safe_date_between_dates(fake_obj, *args, **kwargs):
    date_start = kwargs.get('date_start', kwargs.get('start_date', date(1970, 1, 1)))
    date_end = kwargs.get('date_end', kwargs.get('end_date', date.today()))
    start = _dls_to_date(date_start)
    end = _dls_to_date(date_end)
    if start > end:
        start, end = end, start
    kwargs['date_start'] = start
    kwargs['date_end'] = end
    kwargs.pop('start_date', None)
    kwargs.pop('end_date', None)
    return getattr(fake_obj, 'date_between_dates')(*args, **kwargs)

# Environment variable for output directory
OUTPUT_DIR = os.getenv('OUTPUT_DIR', '.')

def generate_bian_party():
    parties = []
    for _ in range(50000):
        party_id = str(uuid.uuid4())
        full_name = fake.name()
        date_of_birth = fake.date_of_birth(minimum_age=33, maximum_age=53)
        gender = random.choices(['MALE', 'FEMALE', 'OTHER'], weights=[70, 29, 1])[0]
        nationality = random.choices(['US', 'IN', 'AE', 'UK', 'CA'], weights=[50, 20, 10, 10, 10])[0]
        ssn = fake.ssn()
        email = fake.unique.email()
        phone_number = fake.phone_number()
        address_line1 = fake.street_address()
        address_line2 = fake.secondary_address() if random.random() > 0.4 else ''
        city = fake.city()
        state = fake.state()
        postal_code = fake.zipcode()
        country = nationality
        created_at = _dls_safe_date_time_between(fake, start_date='-10y', end_date='now')

        parties.append({
            'PARTY_ID': party_id,
            'FULL_NAME': full_name,
            'DATE_OF_BIRTH': date_of_birth,
            'GENDER': gender,
            'NATIONALITY': nationality,
            'SSN': ssn,
            'EMAIL': email,
            'PHONE_NUMBER': phone_number,
            'ADDRESS_LINE1': address_line1,
            'ADDRESS_LINE2': address_line2,
            'CITY': city,
            'STATE': state,
            'POSTAL_CODE': postal_code,
            'COUNTRY': country,
            'CREATED_AT': created_at
        })
    return parties

def generate_bian_accounts(parties):
    accounts = []
    for _ in range(10000):
        account_id = str(uuid.uuid4())
        party_id = random.choice(parties)['PARTY_ID']
        account_type = random.choices(['Checking', 'Savings', 'Credit Card'], weights=[60, 30, 10])[0]
        account_status = random.choices(['Active', 'Dormant', 'Closed'], weights=[70, 20, 10])[0]
        open_date = _dls_safe_date_between(fake, start_date='-15y', end_date='today')
        if account_type == "Credit Card":
            balance = random.choice([random.uniform(-5000, -100), random.uniform(100, 100000)])
        else:
            balance = random.uniform(100, 100000)
        currency = random.choices(['USD', 'EUR', 'GBP', 'INR', 'AED'], weights=[50, 10, 10, 20, 10])[0]
        interest_rate = random.uniform(0.1, 5.0) if account_type in ['Checking', 'Savings'] else random.uniform(10.0, 25.0)
        branch_id = f"BR{random.randint(1, 50):03}"
        created_at = _dls_safe_date_time_between(fake, start_date=open_date, end_date='now')

        accounts.append({
            'ACCOUNT_ID': account_id,
            'PARTY_ID': party_id,
            'ACCOUNT_TYPE': account_type,
            'ACCOUNT_STATUS': account_status,
            'OPEN_DATE': open_date,
            'BALANCE': balance,
            'CURRENCY': currency,
            'INTEREST_RATE': interest_rate,
            'BRANCH_ID': branch_id,
            'CREATED_AT': created_at
        })
    return accounts

def generate_bian_transaction(accounts):
    transactions = []
    for _ in range(10000):
        transaction_id = str(uuid.uuid4())
        account = random.choice(accounts)
        account_id = account['ACCOUNT_ID']
        transaction_type = random.choices(['DEPOSIT', 'WITHDRAWAL', 'PAYMENT', 'TRANSFER', 'FEE', 'REFUND'], weights=[30, 30, 20, 10, 5, 5])[0]
        amount = random.uniform(1, 5000) if random.random() > 0.2 else random.uniform(1, 20000)
        currency = account['CURRENCY']
        transaction_timestamp = _dls_safe_date_time_between(fake, start_date=datetime.strptime('2010-01-01', '%Y-%m-%d').date(), end_date='now')
        merchant_name = fake.company() if transaction_type not in ['TRANSFER', 'FEE'] else ''
        merchant_category = random.choice(['GROCERY', 'RESTAURANT', 'FUEL', 'ONLINE', 'TRAVEL', 'UTILITY', 'OTHER']) if transaction_type not in ['TRANSFER', 'FEE'] else ''
        location = f"{fake.city()}, {fake.country()}" if transaction_type not in ['TRANSFER', 'FEE'] else ''
        channel = random.choice(['POS', 'ONLINE', 'MOBILE', 'ATM', 'BRANCH'])
        status = random.choices(['POSTED', 'PENDING', 'REVERSED'], weights=[92, 6, 2])[0]
        created_at = _dls_safe_date_time_between(fake, start_date=transaction_timestamp, end_date=transaction_timestamp + timedelta(days=3))

        transactions.append({
            'TRANSACTION_ID': transaction_id,
            'ACCOUNT_ID': account_id,
            'TRANSACTION_TYPE': transaction_type,
            'AMOUNT': amount,
            'CURRENCY': currency,
            'TRANSACTION_TIMESTAMP': transaction_timestamp,
            'MERCHANT_NAME': merchant_name,
            'MERCHANT_CATEGORY': merchant_category,
            'LOCATION': location,
            'CHANNEL': channel,
            'STATUS': status,
            'CREATED_AT': created_at
        })
    return transactions

def generate_bian_kyc_profile(parties):
    kyc_profiles = []
    for _ in range(10000):
        kyc_id = str(uuid.uuid4())
        party_id = random.choice(parties)['PARTY_ID']
        kyc_status = random.choices(['Verified', 'Pending', 'Rejected'], weights=[70, 20, 10])[0]
        review_date = _dls_safe_date_between(fake, start_date='-3y', end_date='today')
        risk_level = random.choices(['High', 'Medium', 'Low'], weights=[25, 50, 25])[0]
        if kyc_status == 'Rejected':
            risk_level = random.choices(['High', 'Medium', 'Low'], weights=[50, 30, 20])[0]
        reviewer_id = f"EMP{random.randint(1001, 1020)}"
        last_updated = _dls_safe_date_time_between(fake, start_date=review_date, end_date='now')

        kyc_profiles.append({
            'KYC_ID': kyc_id,
            'PARTY_ID': party_id,
            'KYC_STATUS': kyc_status,
            'REVIEW_DATE': review_date,
            'RISK_LEVEL': risk_level,
            'REVIEWER_ID': reviewer_id,
            'LAST_UPDATED': last_updated
        })
    return kyc_profiles

def generate_bian_document(parties):
    documents = []
    for _ in range(5000):
        doc_id = str(uuid.uuid4())
        party_id = random.choice(parties)['PARTY_ID']
        doc_type = random.choices(['Passport', 'Driver License', 'National ID'], weights=[50, 30, 20])[0]
        if doc_type == 'Passport':
            doc_number = fake.bothify(text='??#######')
        elif doc_type == 'Driver License':
            doc_number = fake.bothify(text='??########')
        else:
            doc_number = fake.numerify(text='##########')
        issue_date = _dls_safe_date_between(fake, start_date='-15y', end_date='today')
        expiry_date = issue_date + timedelta(days=random.randint(5*365, 10*365))
        issuing_authority = {'Passport': 'Passport Office', 'Driver License': 'DMV', 'National ID': 'National ID Authority'}[doc_type]
        doc_status = 'Expired' if expiry_date < datetime.now().date() else random.choices(['Active', 'Expired'], weights=[85, 15])[0]
        uploaded_at = _dls_safe_date_time_between(fake, start_date=issue_date - timedelta(days=7), end_date=issue_date + timedelta(days=7))

        documents.append({
            'DOC_ID': doc_id,
            'PARTY_ID': party_id,
            'DOC_TYPE': doc_type,
            'DOC_NUMBER': doc_number,
            'ISSUE_DATE': issue_date,
            'EXPIRY_DATE': expiry_date,
            'ISSUING_AUTHORITY': issuing_authority,
            'DOC_STATUS': doc_status,
            'UPLOADED_AT': uploaded_at
        })
    return documents

def generate_bian_risk_assessment(parties):
    risk_assessments = []
    for _ in range(10000):
        risk_id = str(uuid.uuid4())
        party_id = random.choice(parties)['PARTY_ID']
        risk_model_name = random.choice(['MODEL_A', 'MODEL_B', 'MODEL_C'])
        risk_score = random.uniform(0, 100)
        risk_category = 'High' if risk_score > 80 else 'Medium' if risk_score >= 40 else 'Low'
        reviewed_by = f"EMP{random.randint(2001, 2020)}"
        review_date = _dls_safe_date_between(fake, start_date='-2y', end_date='today')
        recommendation = {'High': 'Enhanced Due Diligence', 'Medium': 'Monitor', 'Low': 'Proceed'}[risk_category]
        created_at = _dls_safe_date_time_between(fake, start_date=review_date, end_date='now')

        risk_assessments.append({
            'RISK_ID': risk_id,
            'PARTY_ID': party_id,
            'RISK_MODEL_NAME': risk_model_name,
            'RISK_SCORE': risk_score,
            'RISK_CATEGORY': risk_category,
            'REVIEWED_BY': reviewed_by,
            'REVIEW_DATE': review_date,
            'RECOMMENDATION': recommendation,
            'CREATED_AT': created_at
        })
    return risk_assessments

def generate_bian_device_access(parties):
    device_accesses = []
    for _ in range(50000):
        device_id = str(uuid.uuid4())
        party_id = random.choice(parties)['PARTY_ID']
        device_type = random.choices(['Mobile', 'Desktop', 'Tablet'], weights=[60, 30, 10])[0]
        if device_type == 'Mobile':
            device_os = random.choice(['iOS', 'Android'])
        elif device_type == 'Desktop':
            device_os = random.choice(['Windows', 'macOS', 'Linux'])
        else:
            device_os = random.choice(['iPadOS', 'Android'])
        ip_address = fake.ipv4() if random.random() > 0.5 else fake.ipv6()
        location = f"{fake.city()}, {fake.country()}"
        login_time = _dls_safe_date_time_between(fake, start_date='-6m', end_date='now')
        logout_time = login_time + timedelta(hours=random.uniform(0.1, 8)) if random.random() > 0.05 else None
        auth_method = random.choices(['Biometric', 'Password', 'Two-Factor'], weights=[15, 50, 35])[0]
        session_status = 'Active' if logout_time is None else 'Closed'

        device_accesses.append({
            'DEVICE_ID': device_id,
            'PARTY_ID': party_id,
            'DEVICE_TYPE': device_type,
            'DEVICE_OS': device_os,
            'IP_ADDRESS': ip_address,
            'LOCATION': location,
            'LOGIN_TIME': login_time,
            'LOGOUT_TIME': logout_time,
            'AUTH_METHOD': auth_method,
            'SESSION_STATUS': session_status
        })
    return device_accesses

def generate_bian_fraud_alert(transactions, parties):
    fraud_alerts = []
    for _ in range(17000):
        alert_id = str(uuid.uuid4())
        transaction = random.choice(transactions)
        transaction_id = transaction['TRANSACTION_ID']
        party_id = random.choice(parties)['PARTY_ID']
        alert_type = random.choice(['Unusual Location', 'Multiple Failed Logins', 'High-Value Transaction', 'Suspicious IP'])
        triggered_at = _dls_safe_date_time_between(fake, start_date='-1y', end_date='now')
        resolution_status = random.choices(['Open', 'Resolved', 'Escalated'], weights=[60, 30, 10])[0]
        resolved_by = f"EMP{random.randint(3001, 3050)}" if resolution_status in ['Resolved', 'Escalated'] else None

        fraud_alerts.append({
            'ALERT_ID': alert_id,
            'TRANSACTION_ID': transaction_id,
            'PARTY_ID': party_id,
            'ALERT_TYPE': alert_type,
            'TRIGGERED_AT': triggered_at,
            'RESOLUTION_STATUS': resolution_status,
            'RESOLVED_BY': resolved_by
        })
    return fraud_alerts

def write_csv(filename, data, fieldnames):
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def main():
    parties = generate_bian_party()
    accounts = generate_bian_accounts(parties)
    transactions = generate_bian_transaction(accounts)
    kyc_profiles = generate_bian_kyc_profile(parties)
    documents = generate_bian_document(parties)
    risk_assessments = generate_bian_risk_assessment(parties)
    device_accesses = generate_bian_device_access(parties)
    fraud_alerts = generate_bian_fraud_alert(transactions, parties)

    write_csv('BIAN_PARTY.csv', parties, parties[0].keys())
    write_csv('BIAN_ACCOUNTS.csv', accounts, accounts[0].keys())
    write_csv('BIAN_TRANSACTION.csv', transactions, transactions[0].keys())
    write_csv('BIAN_KYC_PROFILE.csv', kyc_profiles, kyc_profiles[0].keys())
    write_csv('BIAN_DOCUMENT.csv', documents, documents[0].keys())
    write_csv('BIAN_RISK_ASSESSMENT.csv', risk_assessments, risk_assessments[0].keys())
    write_csv('BIAN_DEVICE_ACCESS.csv', device_accesses, device_accesses[0].keys())
    write_csv('BIAN_FRAUD_ALERT.csv', fraud_alerts, fraud_alerts[0].keys())

if __name__ == '__main__':
    main()