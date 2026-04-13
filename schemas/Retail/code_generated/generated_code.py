import os
import csv
import random
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

# Constants
OUTPUT_DIR = os.getenv('OUTPUT_DIR', '.')
CUSTOMER_SEGMENTS = ['NEW', 'REGULAR', 'VIP']
LOYALTY_TIERS = ['BRONZE', 'SILVER', 'GOLD', 'PLATINUM']
ORDER_STATUSES = ['PLACED', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED']
SALES_CHANNELS = ['ONLINE', 'STORE', 'MOBILE']
PAYMENT_METHODS = ['CARD', 'CASH', 'WALLET', 'GIFTCARD']
CURRENCY_CODE = 'USD'

# Ensure OUTPUT_DIR exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_customers(num_customers=2000):
    customers = []
    for customer_id in range(1, num_customers + 1):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.unique.email()
        phone = fake.phone_number()
        customer_segment = random.choices(CUSTOMER_SEGMENTS, weights=[0.3, 0.5, 0.2])[0]
        loyalty_tier = random.choice(LOYALTY_TIERS) if customer_segment != 'NEW' else random.choice(LOYALTY_TIERS + [None])
        city = fake.city()
        state_code = fake.state_abbr()
        country_code = 'US'
        created_at = fake.date_time_this_decade(before_now=True, after_now=False)
        updated_at = _dls_safe_date_time_between(fake, start_date=created_at, end_date='now') if random.random() > 0.5 else None

        customers.append({
            'CUSTOMER_ID': customer_id,
            'FIRST_NAME': first_name,
            'LAST_NAME': last_name,
            'EMAIL': email,
            'PHONE': phone,
            'CUSTOMER_SEGMENT': customer_segment,
            'LOYALTY_TIER': loyalty_tier,
            'CITY': city,
            'STATE_CODE': state_code,
            'COUNTRY_CODE': country_code,
            'CREATED_AT': created_at.isoformat(),
            'UPDATED_AT': updated_at.isoformat() if updated_at else None
        })
    return customers

def generate_orders(customers, num_orders=5000):
    orders = []
    for order_id in range(1, num_orders + 1):
        customer = random.choice(customers)
        customer_id = customer['CUSTOMER_ID']
        order_date = fake.date_time_this_year(before_now=True, after_now=False)
        order_status = random.choices(ORDER_STATUSES, weights=[0.1, 0.1, 0.6, 0.1, 0.1])[0]
        sales_channel = random.choice(SALES_CHANNELS)
        payment_method = random.choice(PAYMENT_METHODS)
        subtotal_amount = round(random.uniform(10.0, 500.0), 2)
        tax_amount = round(subtotal_amount * 0.07, 2)  # Assume 7% tax
        shipping_amount = round(random.uniform(5.0, 20.0), 2)
        total_amount = round(subtotal_amount + tax_amount + shipping_amount, 2)
        shipping_city = fake.city()
        shipping_state_code = fake.state_abbr()
        created_at = _dls_safe_date_time_between(fake, start_date=order_date, end_date='now')

        orders.append({
            'ORDER_ID': order_id,
            'CUSTOMER_ID': customer_id,
            'ORDER_DATE': order_date.isoformat(),
            'ORDER_STATUS': order_status,
            'SALES_CHANNEL': sales_channel,
            'PAYMENT_METHOD': payment_method,
            'CURRENCY_CODE': CURRENCY_CODE,
            'SUBTOTAL_AMOUNT': f"{subtotal_amount:.2f}",
            'TAX_AMOUNT': f"{tax_amount:.2f}",
            'SHIPPING_AMOUNT': f"{shipping_amount:.2f}",
            'TOTAL_AMOUNT': f"{total_amount:.2f}",
            'SHIPPING_CITY': shipping_city,
            'SHIPPING_STATE_CODE': shipping_state_code,
            'CREATED_AT': created_at.isoformat()
        })
    return orders

def write_csv(filename, fieldnames, rows):
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def main():
    customers = generate_customers()
    orders = generate_orders(customers)

    write_csv('RETAIL_CUSTOMERS.csv', customers[0].keys(), customers)
    write_csv('RETAIL_ORDERS.csv', orders[0].keys(), orders)

if __name__ == '__main__':
    main()