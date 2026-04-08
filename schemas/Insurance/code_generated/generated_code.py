import os
import csv
import random
from datetime import datetime, timedelta, date
from faker import Faker
from uuid import uuid4

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

OUTPUT_DIR = os.getenv('OUTPUT_DIR', '.')

def safe_write_csv(filename, rows, fieldnames):
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def generate_insu_customer():
    rows = []
    for _ in range(6500):
        customer_id = str(uuid4())
        customer_type = random.choices(['INDIVIDUAL', 'BUSINESS'], weights=[85, 15])[0]
        if customer_type == 'INDIVIDUAL':
            gender = random.choices(['MALE', 'FEMALE', 'OTHER'], weights=[49, 49, 1])[0]
            first_name = fake.first_name()
            last_name = fake.last_name()
            business_name = 'NOT APPLICABLE'
            dob = fake.date_of_birth(minimum_age=18, maximum_age=63)
        else:
            gender = None
            first_name = 'NOT APPLICABLE'
            last_name = 'NOT APPLICABLE'
            business_name = fake.company()
            dob = None

        email = fake.unique.email()
        phone = fake.phone_number()
        country = random.choices(['US', 'IN', 'AE', 'UK', 'CA'], weights=[50, 20, 10, 10, 10])[0]
        state = fake.state()
        city = fake.city()
        address_line1 = fake.street_address()
        address_line2 = fake.secondary_address()
        postal_code = fake.zipcode()
        registration_date = _dls_safe_date_between(fake, start_date=date(2015, 1, 1), end_date=date.today())
        created_at = _dls_safe_date_time_between(fake, start_date=registration_date, end_date='now')

        rows.append({
            'CUSTOMER_ID': customer_id,
            'CUSTOMER_TYPE': customer_type,
            'FIRST_NAME': first_name,
            'LAST_NAME': last_name,
            'GENDER': gender,
            'BUSINESS_NAME': business_name,
            'DOB': dob,
            'EMAIL': email,
            'PHONE': phone,
            'COUNTRY': country,
            'STATE': state,
            'CITY': city,
            'ADDRESS_LINE1': address_line1,
            'ADDRESS_LINE2': address_line2,
            'POSTAL_CODE': postal_code,
            'REGISTRATION_DATE': registration_date,
            'CREATED_AT': created_at
        })
    return rows

def generate_insu_agent():
    rows = []
    for _ in range(500):
        agent_id = str(uuid4())
        agent_code = f"AGT{fake.random_number(digits=random.randint(7, 9), fix_len=True)}"
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.unique.email()
        phone = fake.phone_number()
        license_no = f"LIC{fake.random_number(digits=random.randint(8, 10), fix_len=True)}"
        country = random.choices(['US', 'IN', 'UK'], weights=[40, 30, 30])[0]
        state = fake.state()
        city = fake.city()
        status = random.choices(['ACTIVE', 'INACTIVE'], weights=[85, 15])[0]
        hire_date = _dls_safe_date_between(fake, start_date=date(2010, 1, 1), end_date=date.today())
        created_at = _dls_safe_date_time_between(fake, start_date=hire_date, end_date='now')

        rows.append({
            'AGENT_ID': agent_id,
            'AGENT_CODE': agent_code,
            'FIRST_NAME': first_name,
            'LAST_NAME': last_name,
            'EMAIL': email,
            'PHONE': phone,
            'LICENSE_NO': license_no,
            'COUNTRY': country,
            'STATE': state,
            'CITY': city,
            'STATUS': status,
            'HIRE_DATE': hire_date,
            'CREATED_AT': created_at
        })
    return rows

def generate_insu_policy(customers, agents):
    rows = []
    for _ in range(10000):
        policy_id = str(uuid4())
        policy_number = f"PL{datetime.now().year}{fake.random_number(digits=random.randint(8, 10), fix_len=True)}"
        customer_id = random.choice(customers)['CUSTOMER_ID']
        product_id = str(uuid4())  # Placeholder for product ID
        agent_id = random.choice(agents)['AGENT_ID'] if random.random() < 0.9 else None
        status = random.choices(['ACTIVE', 'LAPSED', 'CANCELED', 'EXPIRED'], weights=[70, 10, 10, 10])[0]
        effective_date = _dls_safe_date_between(fake, start_date=date(2016, 1, 1), end_date=date.today())
        expiration_date = effective_date + timedelta(days=random.randint(305, 425))
        premium_billing_freq = random.choices(['MONTHLY', 'QUARTERLY', 'ANNUAL'], weights=[70, 20, 10])[0]
        premium_amount = round(random.uniform(300, 2000) * random.uniform(0.85, 1.15), 2)
        currency = random.choices(['USD', 'INR', 'AED', 'GBP', 'CAD'], weights=[50, 20, 10, 10, 10])[0]
        created_at = _dls_safe_date_time_between(fake, start_date=effective_date, end_date=min(_dls_to_datetime(expiration_date), datetime.now()))

        rows.append({
            'POLICY_ID': policy_id,
            'POLICY_NUMBER': policy_number,
            'CUSTOMER_ID': customer_id,
            'PRODUCT_ID': product_id,
            'AGENT_ID': agent_id,
            'STATUS': status,
            'EFFECTIVE_DATE': effective_date,
            'EXPIRATION_DATE': expiration_date,
            'PREMIUM_BILLING_FREQ': premium_billing_freq,
            'PREMIUM_AMOUNT': premium_amount,
            'CURRENCY': currency,
            'CREATED_AT': created_at
        })
    return rows

def generate_insu_risk_object(policies):
    rows = []
    for _ in range(16500):
        object_id = str(uuid4())
        policy_id = random.choice(policies)['POLICY_ID']
        object_type = random.choices(['VEHICLE', 'PROPERTY', 'PERSON', 'EQUIPMENT'], weights=[50, 35, 10, 5])[0]
        description = fake.sentence(nb_words=random.randint(4, 10))
        year_made = random.randint(1980, 2025) if object_type in ['VEHICLE', 'EQUIPMENT'] else None
        serial_no = fake.bothify(text='??#######') if object_type in ['VEHICLE', 'PROPERTY'] else None
        address_line1 = fake.street_address() if object_type == 'PROPERTY' or random.random() < 0.3 else None
        address_line2 = fake.secondary_address() if address_line1 else None
        city = fake.city() if address_line1 else None
        state = fake.state() if address_line1 else None
        postal_code = fake.zipcode() if address_line1 else None
        country = fake.country() if address_line1 else None
        sum_insured = round(random.uniform(20000, 2000000), 2)
        policy = next((p for p in policies if p['POLICY_ID'] == policy_id), None)
        created_at = _dls_safe_date_time_between(fake, start_date=policy['EFFECTIVE_DATE'], end_date=min(_dls_to_datetime(policy['EXPIRATION_DATE']), datetime.now()))

        rows.append({
            'OBJECT_ID': object_id,
            'POLICY_ID': policy_id,
            'OBJECT_TYPE': object_type,
            'DESCRIPTION': description,
            'YEAR_MADE': year_made,
            'SERIAL_NO': serial_no,
            'ADDRESS_LINE1': address_line1,
            'ADDRESS_LINE2': address_line2,
            'CITY': city,
            'STATE': state,
            'POSTAL_CODE': postal_code,
            'COUNTRY': country,
            'SUM_INSURED': sum_insured,
            'CREATED_AT': created_at
        })
    return rows

def generate_insu_claim(policies, customers):
    rows = []
    for _ in range(3500):
        claim_id = str(uuid4())
        claim_number = f"CL{fake.random_number(digits=random.randint(9, 12), fix_len=True)}"
        policy = random.choice(policies)
        policy_id = policy['POLICY_ID']
        customer_id = policy['CUSTOMER_ID']
        claim_type = random.choices(['ACCIDENT', 'THEFT', 'FIRE', 'MEDICAL', 'NATURAL DISASTER', 'LIABILITY', 'OTHERS'],
                                    weights=[45, 10, 8, 15, 7, 10, 5])[0]
        incident_date = _dls_safe_date_between(fake, start_date=policy['EFFECTIVE_DATE'], end_date=min(_dls_to_date(policy['EXPIRATION_DATE']), date.today()))
        reported_date = incident_date + timedelta(days=random.randint(0, 30))
        status = random.choices(['OPEN', 'IN-REVIEW', 'APPROVED', 'REJECTED', 'CLOSED', 'WITHDRAWN'],
                                weights=[25, 20, 25, 10, 18, 2])[0]
        loss_estimate = round(random.uniform(500, 500000), 2)
        deductible = round(random.uniform(0, 5000), 2)
        approved_amount = round(loss_estimate * random.uniform(0.5, 1.0), 2) if status in ['APPROVED', 'CLOSED'] else 0 if status == 'REJECTED' else None
        closed_date = reported_date + timedelta(days=random.randint(0, 365)) if status == 'CLOSED' else None
        created_at = datetime.now()

        rows.append({
            'CLAIM_ID': claim_id,
            'CLAIM_NUMBER': claim_number,
            'POLICY_ID': policy_id,
            'CUSTOMER_ID': customer_id,
            'CLAIM_TYPE': claim_type,
            'INCIDENT_DATE': incident_date,
            'REPORTED_DATE': reported_date,
            'STATUS': status,
            'LOSS_ESTIMATE': loss_estimate,
            'DEDUCTIBLE': deductible,
            'APPROVED_AMOUNT': approved_amount,
            'CLOSED_DATE': closed_date,
            'CREATED_AT': created_at
        })
    return rows

def generate_insu_payment(policies, claims):
    rows = []
    for _ in range(13500):
        payment_id = str(uuid4())
        policy = random.choice(policies)
        policy_id = policy['POLICY_ID']
        if random.random() < 0.3:
            claim = random.choice(claims)
            claim_id = claim['CLAIM_ID']
            direction = 'Payout'
            payment_date = _dls_safe_date_between(fake, start_date=claim['REPORTED_DATE'], end_date=(claim['CLOSED_DATE'] or claim['REPORTED_DATE'] + timedelta(days=60)))
        else:
            claim_id = None
            direction = random.choices(['Premium', 'Refund'], weights=[93, 7])[0]
            payment_date = _dls_safe_date_between(fake, start_date=policy['EFFECTIVE_DATE'], end_date=policy['EXPIRATION_DATE'])

        status = random.choices(['Completed', 'Pending', 'Failed'], weights=[90, 7, 3])[0]
        method = random.choices(['ACH', 'Card', 'Check', 'Wire', 'Cash', 'UPI'], weights=[35, 30, 10, 15, 5, 5])[0]
        amount = round(policy['PREMIUM_AMOUNT'] * random.uniform(0.8, 1.2), 2)
        currency = policy['CURRENCY']
        transaction_ref = f"TX{fake.random_number(digits=random.randint(10, 14), fix_len=True)}"
        created_at = _dls_safe_date_time_between(fake, start_date=payment_date, end_date=datetime.now())

        rows.append({
            'PAYMENT_ID': payment_id,
            'POLICY_ID': policy_id,
            'CLAIM_ID': claim_id,
            'DIRECTION': direction,
            'STATUS': status,
            'METHOD': method,
            'AMOUNT': amount,
            'CURRENCY': currency,
            'PAYMENT_DATE': payment_date,
            'TRANSACTION_REF': transaction_ref,
            'CREATED_AT': created_at
        })
    return rows

def main():
    customers = generate_insu_customer()
    agents = generate_insu_agent()
    policies = generate_insu_policy(customers, agents)
    risk_objects = generate_insu_risk_object(policies)
    claims = generate_insu_claim(policies, customers)
    payments = generate_insu_payment(policies, claims)

    safe_write_csv('INSU_CUSTOMER.csv', customers, customers[0].keys())
    safe_write_csv('INSU_AGENT.csv', agents, agents[0].keys())
    safe_write_csv('INSU_POLICY.csv', policies, policies[0].keys())
    safe_write_csv('INSU_RISK_OBJECT.csv', risk_objects, risk_objects[0].keys())
    safe_write_csv('INSU_CLAIM.csv', claims, claims[0].keys())
    safe_write_csv('INSU_PAYMENT.csv', payments, payments[0].keys())

if __name__ == '__main__':
    main()