import os
from dotenv import load_dotenv
import urllib.parse
import pandas as pd
import numpy as np
import random
from faker import Faker
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import uuid
import math

# --- LOAD ENVIRONMENT VARIABLES ---
load_dotenv(override=True) # This reads the .env file in your current directory

# --- CONFIGURATION ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "streamify_inc")
DB_USER = os.getenv("DB_USER", "postgres")

# [FIX] Load the password and then URL-encode it
raw_password = os.getenv("DB_PASSWORD", "")
DB_PASSWORD = urllib.parse.quote_plus(raw_password)

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ============================
# DATA GENERATION VARIABLES
# ============================

# ---- USERS ----
NUM_USERS = 10000
COUNTRIES = ["NG", "GH", "KE", "ZA", "UK", "PT", "PL"]

ACQUISITION_CHANNELS = [
    "organic",
    "referral",
    "google_ads",
    "instagram_ads",
    "partnerships",
    "tiktok_ads"
]

ACQUISITION_CHANNEL_WEIGHTS = [
    0.35,  # organic
    0.20,  # referral
    0.15,  # google_ads
    0.15,  # instagram_ads
    0.10,  # partnerships
    0.05   # tiktok_ads
]

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 12, 31)


# ---- EVENTS ----
# Onboarding funnel probabilities
KYC_START_RATE = 0.95        # % of users who start KYC
KYC_PASS_RATE = 0.85         # % who complete KYC
ADD_MONEY_CLICK_RATE = 0.70  # % who click "add money"
ACTIVATION_RATE = 0.60       # % who become active (deposit/transfer/etc.)

# Timing controls
MIN_HOURS_TO_KYC_START = 1
MAX_HOURS_TO_KYC_START = 48

MIN_HOURS_TO_KYC_COMPLETE = 1
MAX_HOURS_TO_KYC_COMPLETE = 24

MIN_DAYS_TO_ADD_MONEY = 1
MAX_DAYS_TO_ADD_MONEY = 7

# Platform distributions
PLATFORMS = ["android", "ios", "web"]
PLATFORM_WEIGHTS = [0.50, 0.35, 0.15]

DEVICE_TYPES = ["mobile", "desktop"]
DEVICE_WEIGHTS = [0.85, 0.15]

# Engagement events (optional extra sessions)
MIN_SESSION_EVENTS = 3
MAX_SESSION_EVENTS = 20


# ---- TRANSACTIONS ----
AVG_TXNS_PER_USER = 10       # base average
HIGH_VALUE_USERS_PCT = 0.20      # 20% of users transact more
HIGH_VALUE_MULTIPLIER = 3        # high-value users make 3× the base

# Transaction types
TXN_TYPES = ["deposit", "withdrawal", "transfer", "merchant_payment"]
TXN_TYPE_WEIGHTS = [0.50, 0.10, 0.30, 0.10]

# Amount behavior
TXN_AMOUNT_MEAN = 5000       # for lognormal (mean in Naira or user currency)
TXN_AMOUNT_SIGMA = 1.0       # spread/variance
# [FIX] Added log of the mean for correct lognormvariate usage
TXN_AMOUNT_LOG_MEAN = math.log(TXN_AMOUNT_MEAN)

# Fees
FEE_PERCENT = 0.015      # 1.5% fee
DEPOSIT_FAILURE_RATE = 0.05  # 5% failed deposits (optional)

CURRENCIES = ["NGN", "USD", "GBP", "EUR"]
CURRENCY_WEIGHTS = [0.70, 0.10, 0.10, 0.10]


# ---- MARKETING SPEND ----
MARKETING_CHANNELS = ACQUISITION_CHANNELS    # reuse channels
DAILY_SPEND_MIN = 1000
DAILY_SPEND_MAX = 20000


# Initialize Faker
fake = Faker()

# --- HELPER FUNCTIONS ---
def get_db_engine():
    """Creates a SQLAlchemy engine."""
    try:
        engine = create_engine(DATABASE_URL)
        # Test connection
        with engine.connect() as conn:
            print("Database connection successful.")
        return engine
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

def load_df_to_db(df, table_name, engine):
    """Loads a DataFrame into a specified table."""
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Successfully loaded {len(df)} records into '{table_name}'.")
    except Exception as e:
        print(f"Error loading data into '{table_name}': {e}")

# --- DATA GENERATION FUNCTIONS ---

def generate_users_data(n):
    print(f"Generating {n} users...")
    data = []
    for _ in range(n):
        data.append({
            'user_id': str(uuid.uuid4()),
            'signup_date': fake.date_time_between(start_date=START_DATE, end_date=END_DATE, tzinfo=None),
            'country': random.choice(COUNTRIES),
            'acquisition_channel': random.choices(population=ACQUISITION_CHANNELS, weights=ACQUISITION_CHANNEL_WEIGHTS, k=1)[0]
        })
    return pd.DataFrame(data)

def generate_events_stream_data(df_users):
    print(f"Generating events stream data for {len(df_users)} users...")
    data = []    

    for index, user in df_users.iterrows():
        user_id = user['user_id']
        signup_date = user['signup_date']

        # KYC Start
        if random.random() < KYC_START_RATE:
            kyc_start_time = signup_date + timedelta(hours=random.randint(MIN_HOURS_TO_KYC_START, MAX_HOURS_TO_KYC_START))
            data.append({
                'event_id': str(uuid.uuid4()),
                'user_id': user_id,
                'event_name': 'kyc_start',
                'event_timestamp': kyc_start_time,
                'platform': random.choices(population=PLATFORMS, weights=PLATFORM_WEIGHTS, k=1)[0],
                'device_type': random.choices(population=DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1)[0]
            })
            
            # KYC Complete
            if random.random() < KYC_PASS_RATE:
                kyc_complete_time = kyc_start_time + timedelta(hours=random.randint(MIN_HOURS_TO_KYC_COMPLETE, MAX_HOURS_TO_KYC_COMPLETE))
                data.append({
                    'event_id': str(uuid.uuid4()),
                    'user_id': user_id,
                    'event_name': 'kyc_complete',
                    'event_timestamp': kyc_complete_time,
                    'platform': random.choices(population=PLATFORMS, weights=PLATFORM_WEIGHTS, k=1)[0],
                    'device_type': random.choices(population=DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1)[0]
                })
                
                # Add Money Click
                if random.random() < ADD_MONEY_CLICK_RATE:
                    add_money_time = kyc_complete_time + timedelta(days=random.randint(MIN_DAYS_TO_ADD_MONEY, MAX_DAYS_TO_ADD_MONEY))
                    data.append({
                        'event_id': str(uuid.uuid4()),
                        'user_id': user_id,
                        'event_name': 'add_money_click',
                        'event_timestamp': add_money_time,
                        'platform': random.choices(population=PLATFORMS, weights=PLATFORM_WEIGHTS, k=1)[0],
                        'device_type': random.choices(population=DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1)[0]
                    })
                    
                    # Activation
                    if random.random() < ACTIVATION_RATE:
                        activation_time = add_money_time + timedelta(days=random.randint(0, 3))
                        data.append({
                            'event_id': str(uuid.uuid4()),
                            'user_id': user_id,
                            'event_name': 'activation',
                            'event_timestamp': activation_time,
                            'platform': random.choices(population=PLATFORMS, weights=PLATFORM_WEIGHTS, k=1)[0],
                            'device_type': random.choices(population=DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1)[0]
                        })
    return pd.DataFrame(data)

def generate_transactions_data(df_users):
    print(f"Generating transaction data for {len(df_users)} users...")
    data = []
    
    for index, user in df_users.iterrows():
        user_id = user['user_id']
        signup_date = user['signup_date']

        # Determine if user is high-value
        is_high_value = random.random() < HIGH_VALUE_USERS_PCT
        num_txns = np.random.poisson(AVG_TXNS_PER_USER * (HIGH_VALUE_MULTIPLIER if is_high_value else 1))
        
        for _ in range(num_txns):
            txn_type = random.choices(population=TXN_TYPES, weights=TXN_TYPE_WEIGHTS, k=1)[0]
            
            
            amount = round(random.lognormvariate(TXN_AMOUNT_LOG_MEAN, TXN_AMOUNT_SIGMA), 2)
            
            currency = random.choices(population=CURRENCIES, weights=CURRENCY_WEIGHTS, k=1)[0]
            fee = round(amount * FEE_PERCENT, 2) if txn_type == 'deposit' else 0.0

            data.append({
                'transaction_id': str(uuid.uuid4()),
                'user_id': user_id,
                'timestamp': fake.date_time_between(start_date=signup_date, end_date=END_DATE, tzinfo=None),
                'amount': amount,
                'currency': currency,
                'type': txn_type,
                'fee': fee
            })
    return pd.DataFrame(data)

def generate_marketing_spend_data():
   print("Generating marketing spend data...")
   data = []
   for single_date in pd.date_range(start=START_DATE, end=END_DATE):
        for channel in MARKETING_CHANNELS:
            daily_spend = round(random.uniform(DAILY_SPEND_MIN, DAILY_SPEND_MAX), 2)
            data.append({
                'spend_id': str(uuid.uuid4()),
                'date': single_date.date(),
                'channel': channel,
                'amount_spent': daily_spend
            })
   return pd.DataFrame(data)

# --- MAIN EXECUTION ---
def main():
    print("--- Starting Data Ingestion ---")
    engine = get_db_engine()

    # Generate data in order of dependency
    df_users = generate_users_data(NUM_USERS)

    df_events_stream = generate_events_stream_data(df_users)
    df_txn_data = generate_transactions_data(df_users)
    df_marketing_spend = generate_marketing_spend_data()

    # Load all dataframes to Postgres
    load_df_to_db(df_users, 'users', engine)
    load_df_to_db(df_events_stream, 'events', engine)
    load_df_to_db(df_txn_data, 'transactions', engine)
    load_df_to_db(df_marketing_spend, 'marketing_spend', engine)

    print("--- Data Ingestion Complete ---")

if __name__ == "__main__":
    main()