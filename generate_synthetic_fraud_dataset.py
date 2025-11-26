"""
generate_synthetic_fraud_dataset.py

Генерация синтетического, но логически согласованного датасета транзакций для MVP anti-fraud.

Ключевые моменты:
- 50_000 уникальных users (user_id)
- для каждого user от 1 до 250 транзакций (по умолчанию среднее контролируется параметром)
- процент мошеннических транзакций не превышает target_fraud_rate (по умолчанию 0.02 -> 2%, гарантирован <= 0.03)
- вычисляются поведенческие признаки (prev counts, avg_amount windows, velocity)
- метки fraud генерируются через rule-based скор и пост-обработку для точного контроля доли мошенничества
"""

import os
import random
import hashlib
from datetime import datetime, timedelta
from tqdm import tqdm

import numpy as np
import pandas as pd
from faker import Faker
from scipy.stats import lognorm, truncnorm
from sklearn.preprocessing import MinMaxScaler

# --------------------------
# CONFIG
# --------------------------
CONFIG = {
    "n_users": 5000, # Reduced from 50,000 for MVP speed
    "avg_tx_per_user": 10, # Reduced for MVP speed
    "min_tx_per_user": 1,
    "max_tx_per_user": 50,
    "start_date": datetime.utcnow() - timedelta(days=30), # Reduced history
    "end_date": datetime.utcnow(),
    "target_fraud_rate": 0.02,
    "max_allowed_fraud_rate": 0.03,
    "n_merchants": 1000,
    "merchant_high_risk_pct": 0.05,
    "seed": 42,
    "output_dir": "output",
    "save_parquet": False, # We will handle saving externally if needed
    "sample_preview_rows": 1000,
}

# --------------------------
# Reproducibility
# --------------------------
random.seed(CONFIG["seed"])
np.random.seed(CONFIG["seed"])
fake = Faker()
Faker.seed(CONFIG["seed"])

# --------------------------
# Utility helpers
# --------------------------
def make_user_id(i):
    return f"user_{i:06d}"

def make_txn_id(i):
    return f"txn_{i:09d}"

def hash_ip(s):
    return hashlib.sha1(s.encode()).hexdigest()[:16]

def truncated_normal(mean=0, sd=1, low=0, high=100, size=1):
    a, b = (low - mean) / sd, (high - mean) / sd
    return truncnorm.rvs(a, b, loc=mean, scale=sd, size=size)

# --------------------------
# 1) Генерация профилей users
# --------------------------
def generate_users(n_users):
    users = []
    countries = ["US", "GB", "DE", "FR", "RU", "KZ", "IN", "CN", "BR", "NG"]
    timezones = {
        "US": "America/New_York",
        "GB": "Europe/London",
        "DE": "Europe/Berlin",
        "FR": "Europe/Paris",
        "RU": "Europe/Moscow",
        "KZ": "Asia/Almaty",
        "IN": "Asia/Kolkata",
        "CN": "Asia/Shanghai",
        "BR": "America/Sao_Paulo",
        "NG": "Africa/Lagos"
    }
    device_choices = ["mobile", "desktop", "tablet"]
    for i in range(n_users):
        user_id = make_user_id(i+1)
        country = random.choices(countries, weights=[30,8,6,6,10,4,8,10,10,8], k=1)[0]
        city = fake.city()
        timezone = timezones.get(country, "UTC")
        age = int(np.clip(truncated_normal(mean=40, sd=12, low=18, high=85, size=1)[0], 18, 85))
        tenure_days = int(abs(truncated_normal(mean=800, sd=500, low=1, high=5000, size=1)[0]))
        registration_date = CONFIG["start_date"] - timedelta(days=tenure_days)
        credit_score = int(np.clip(truncated_normal(mean=650, sd=80, low=300, high=850, size=1)[0],300,850))
        base_balance = max(0, (credit_score - 300) * 10 + age * 50 + np.random.normal(0,2000))
        account_balance = float(round(base_balance + np.random.normal(0, 5000),2))
        typical_spend = max(1.0, account_balance * 0.005 + np.random.exponential(10))
        device_type = random.choices(device_choices, weights=[0.6, 0.3, 0.1], k=1)[0]
        os_name = random.choice(["Windows", "macOS", "Linux", "Android", "iOS"])
        browser = random.choice(["Chrome", "Firefox", "Safari", "Edge"])
        users.append({
            "user_id": user_id,
            "home_country": country,
            "home_city": city,
            "timezone": timezone,
            "user_age": age,
            "user_tenure_days": tenure_days,
            "registration_date": registration_date.isoformat(),
            "credit_score": credit_score,
            "account_balance": round(account_balance, 2),
            "typical_spend": round(typical_spend, 2),
            "device_type": device_type,
            "os": os_name,
            "browser": browser
        })
    return pd.DataFrame(users)

# --------------------------
# 2) Генерация merchants
# --------------------------
def generate_merchants(n_merchants):
    categories = ["electronics", "groceries", "travel", "entertainment", "fashion", "services", "gas", "food_delivery"]
    merchants = []
    for i in range(n_merchants):
        m_id = f"m_{i+1:05d}"
        category = random.choices(categories, weights=[10,20,8,10,12,15,8,17], k=1)[0]
        base = {"electronics": 0.15, "groceries": 0.03, "travel":0.12, "entertainment":0.06,
                "fashion":0.07, "services":0.05, "gas":0.04, "food_delivery":0.08}.get(category, 0.05)
        high_risk = random.random() < CONFIG["merchant_high_risk_pct"]
        merchant_risk_score = min(0.95, max(0.01, np.random.beta(2, 20) + (0.25 if high_risk else 0) + base))
        is_high_risk = high_risk or (merchant_risk_score > 0.3)
        name = fake.company()
        city = fake.city()
        mcc_code = random.randint(3000, 5999)
        merchants.append({
            "merchant_id": m_id,
            "merchant_name": name,
            "merchant_category": category,
            "merchant_risk_score": round(merchant_risk_score, 3),
            "is_high_risk_merchant": int(is_high_risk),
            "merchant_city": city,
            "mcc_code": mcc_code
        })
    return pd.DataFrame(merchants)

# --------------------------
# 3) Генерация транзакций (raw)
# --------------------------
def generate_transactions(users_df, merchants_df):
    user_count = len(users_df)
    tx_records = []
    txn_global_idx = 0

    currencies = ["USD","EUR","GBP","KZT","RUB"]
    exch = {"USD":1.0, "EUR":1.05, "GBP":1.25, "KZT":0.0023, "RUB":0.012}
    country_to_currency = {
        "US":"USD","GB":"GBP","DE":"EUR","FR":"EUR","RU":"RUB","KZ":"KZT","IN":"USD","CN":"USD","BR":"USD","NG":"USD"
    }

    start_ts = CONFIG["start_date"].timestamp()
    end_ts = CONFIG["end_date"].timestamp()

    avg_tx = CONFIG["avg_tx_per_user"]

    for idx, u in tqdm(users_df.iterrows(), total=user_count, desc="users"):
        n = np.random.poisson(lam=avg_tx)
        n = int(np.clip(n, CONFIG["min_tx_per_user"], CONFIG["max_tx_per_user"]))
        user_id = u["user_id"]
        typical_spend = max(1.0, float(u["typical_spend"]))
        home_country = u["home_country"]
        timezone = u["timezone"]
        device_pref = u["device_type"]
        travel_prob = 0.02 if random.random() < 0.9 else 0.12
        
        ts = np.random.uniform(start_ts, end_ts, size=n)
        if random.random() < 0.05:
            burst_center = np.random.uniform(start_ts, end_ts)
            burst_size = max(2, int(0.05 * n))
            burst_times = np.random.normal(loc=burst_center, scale=3600*6, size=burst_size)
            ts[:burst_size] = burst_times
        ts = np.clip(ts, start_ts, end_ts)
        ts = np.sort(ts)

        user_scale = typical_spend
        user_std_factor = max(0.3, np.random.rand()*1.0)

        for t in ts:
            txn_global_idx += 1
            txn_id = make_txn_id(txn_global_idx)
            timestamp = datetime.utcfromtimestamp(t)
            s = 1.0 * user_std_factor
            amount = lognorm(s=s, scale=user_scale).rvs()
            if random.random() < 0.002:
                amount *= random.uniform(10, 50)

            if random.random() < travel_prob:
                country = random.choice(list(country_to_currency.keys()))
            else:
                country = home_country

            currency = country_to_currency.get(country, "USD")
            amount_usd = amount * exch.get(currency, 1.0)

            merchant = merchants_df.sample(1).iloc[0]
            merchant_id = merchant["merchant_id"]
            merchant_category = merchant["merchant_category"]
            merchant_risk = merchant["merchant_risk_score"]
            mcc = merchant["mcc_code"]
            merchant_city = merchant["merchant_city"]

            ip_raw = fake.ipv4()
            ip_address = hash_ip(ip_raw)

            device_type = device_pref if random.random() < 0.85 else random.choice(["mobile","desktop","tablet"])
            os_name = random.choice(["Windows","macOS","Linux","Android","iOS"])
            browser = random.choice(["Chrome","Firefox","Safari","Edge"])

            txn_record = {
                "transaction_id": txn_id,
                "user_id": user_id,
                "merchant_id": merchant_id,
                "timestamp": timestamp.isoformat(),
                "transaction_date": timestamp.date().isoformat(),
                "transaction_hour": timestamp.hour,
                "amount": round(float(amount),2),
                "currency": currency,
                "amount_usd": round(float(amount_usd),2),
                "country": country,
                "city": merchant_city,
                "ip_address": ip_address,
                "timezone": timezone,
                "device_type": device_type,
                "os": os_name,
                "browser": browser,
                "merchant_category": merchant_category,
                "merchant_risk_score": merchant_risk,
                "is_high_risk_merchant": int(merchant["is_high_risk_merchant"]),
                "mcc_code": mcc,
            }
            tx_records.append(txn_record)
    tx_df = pd.DataFrame(tx_records)
    return tx_df

# --------------------------
# 4) Вычисление агрегированных/поведенческих признаков
# --------------------------
def compute_behavioral_features(tx_df):
    tx_df["timestamp"] = pd.to_datetime(tx_df["timestamp"])
    tx_df.sort_values(["user_id","timestamp"], inplace=True)
    tx_df.reset_index(drop=True, inplace=True)

    n = len(tx_df)
    prev_count_1h = np.zeros(n, dtype=int)
    prev_count_24h = np.zeros(n, dtype=int)
    avg_amount_7d = np.zeros(n, dtype=float)
    total_spent_30d = np.zeros(n, dtype=float)
    hours_since_last = np.zeros(n, dtype=float)
    amount_deviation = np.zeros(n, dtype=float)
    velocity_1h = np.zeros(n, dtype=float)
    amount_velocity = np.zeros(n, dtype=float)

    grouped = tx_df.groupby("user_id", sort=False)
    for user, idxs in tqdm(grouped.groups.items(), total=len(grouped), desc="computing behavioral"):
        indices = idxs
        times = tx_df.loc[indices, "timestamp"].values.astype("datetime64[ns]").astype("int64") // 10**9
        amounts = tx_df.loc[indices, "amount"].values.astype(float)
        cumsum_amounts = np.cumsum(amounts)
        
        for i_pos, global_i in enumerate(indices):
            t = times[i_pos]
            t_1h = t - 3600
            t_24h = t - 86400
            t_7d = t - 7*86400
            t_30d = t - 30*86400

            left_1h = np.searchsorted(times, t_1h, side="right")
            left_24h = np.searchsorted(times, t_24h, side="right")
            left_7d = np.searchsorted(times, t_7d, side="right")
            left_30d = np.searchsorted(times, t_30d, side="right")

            prev_count_1h[global_i] = i_pos - left_1h
            prev_count_24h[global_i] = i_pos - left_24h

            if i_pos - left_7d > 0:
                sum7 = cumsum_amounts[i_pos-1] - (cumsum_amounts[left_7d-1] if left_7d-1 >=0 else 0)
                avg_amount_7d[global_i] = sum7 / (i_pos - left_7d)
            else:
                avg_amount_7d[global_i] = amounts[i_pos]

            if i_pos - left_30d > 0:
                sum30 = cumsum_amounts[i_pos-1] - (cumsum_amounts[left_30d-1] if left_30d-1 >= 0 else 0)
                total_spent_30d[global_i] = sum30
            else:
                total_spent_30d[global_i] = amounts[i_pos]

            if i_pos == 0:
                hours_since_last[global_i] = np.nan
            else:
                dt = (times[i_pos] - times[i_pos-1]) / 3600.0
                hours_since_last[global_i] = dt

            if i_pos == 0:
                amount_deviation[global_i] = 0.0
            else:
                mu = np.mean(amounts[:i_pos])
                sd = np.std(amounts[:i_pos]) if np.std(amounts[:i_pos]) > 0 else 1.0
                amount_deviation[global_i] = (amounts[i_pos] - mu) / sd

            avg_24h = (i_pos / ((times[i_pos] - times[0]) / 86400.0 + 1e-6)) if (times[i_pos] - times[0])>0 else 0.0
            velocity_1h[global_i] = prev_count_1h[global_i] / (avg_24h + 1e-6)

            if i_pos == 0:
                amount_velocity[global_i] = 0.0
            else:
                prev_amt = amounts[i_pos-1]
                amount_velocity[global_i] = (amounts[i_pos] - prev_amt) / (prev_amt + 1e-6)

    tx_df["prev_transaction_count_1h"] = prev_count_1h
    tx_df["prev_transaction_count_24h"] = prev_count_24h
    tx_df["avg_transaction_amount_7d"] = np.round(avg_amount_7d,2)
    tx_df["total_spent_30d"] = np.round(total_spent_30d,2)
    tx_df["hours_since_last_transaction"] = np.round(hours_since_last,3)
    tx_df["amount_deviation"] = np.round(amount_deviation,3)
    tx_df["velocity_1h"] = np.round(velocity_1h,3)
    tx_df["amount_velocity"] = np.round(amount_velocity,3)

    user_counts = tx_df.groupby("user_id").size().rename("tx_count")
    user_days = tx_df.groupby("user_id").agg(min_ts=("timestamp","min"), max_ts=("timestamp","max"))
    user_days["days_active"] = (user_days["max_ts"] - user_days["min_ts"]).dt.total_seconds() / (3600*24) + 1e-6
    freq = (user_counts / user_days["days_active"]).to_dict()
    tx_df["transaction_frequency"] = tx_df["user_id"].map(freq).round(3)

    return tx_df

# --------------------------
# 5) Генерация метки is_fraud
# --------------------------
def assign_fraud_labels(tx_df, target_rate=0.02, max_allowed=0.03):
    base_prob = 0.002
    score = np.zeros(len(tx_df), dtype=float)

    score += np.clip(tx_df["amount_deviation"].fillna(0) / 4.0, 0, 1) * 1.2
    score += tx_df["merchant_risk_score"].fillna(0) * 2.0
    night = tx_df["transaction_hour"].isin([0,1,2,3,4,5])
    score += night.astype(float) * 0.6
    score += np.clip(tx_df["prev_transaction_count_1h"] / 5.0, 0, 2.0) * 0.6
    score += np.clip(tx_df["velocity_1h"], 0, 5) * 0.6
    score += np.clip((tx_df["amount"] / (tx_df["avg_transaction_amount_7d"] + 1e-6)) / 10.0, 0, 2.0) * 0.8
    score += np.random.beta(1, 50, size=len(tx_df)) * 0.2

    probs = 1 / (1 + np.exp(- (score * 1.0)))
    final_prob = base_prob + (probs * (0.08))
    final_prob = np.clip(final_prob, 0, 0.95)

    random_vals = np.random.rand(len(final_prob))
    is_fraud = (random_vals < final_prob).astype(int)
    tx_df["fraud_score"] = np.round(final_prob,4)
    tx_df["is_fraud"] = is_fraud

    current_rate = tx_df["is_fraud"].mean()
    if current_rate > max_allowed:
        fraud_idx = tx_df.index[tx_df["is_fraud"]==1].to_numpy()
        n_target = int(max_allowed * len(tx_df))
        n_to_keep = max(0, n_target)
        fraud_scores = tx_df.loc[fraud_idx, "fraud_score"].values
        order = np.argsort(-fraud_scores)
        keep_idx = fraud_idx[order][:n_to_keep]
        tx_df.loc[tx_df["is_fraud"]==1, "is_fraud"] = 0
        tx_df.loc[keep_idx, "is_fraud"] = 1
    else:
        if current_rate < target_rate:
            n_total = len(tx_df)
            n_target = int(target_rate * n_total)
            n_need = n_target - int(current_rate * n_total)
            if n_need > 0:
                nonfraud_idx = tx_df.index[tx_df["is_fraud"]==0].to_numpy()
                nonfraud_scores = tx_df.loc[nonfraud_idx, "fraud_score"].values
                order = np.argsort(-nonfraud_scores)
                pick_idx = nonfraud_idx[order][:n_need]
                tx_df.loc[pick_idx, "is_fraud"] = 1
    return tx_df

# --------------------------
# 6) Top-level runner
# --------------------------
def run_generation():
    # os.makedirs(CONFIG["output_dir"], exist_ok=True) # Managed by caller
    print("Generating users...")
    users_df = generate_users(CONFIG["n_users"])
    print("Generating merchants...")
    merchants_df = generate_merchants(CONFIG["n_merchants"])

    print("Generating raw transactions...")
    tx_df = generate_transactions(users_df, merchants_df)

    print("Merging metadata...")
    users_meta = users_df.set_index("user_id")[["home_country","timezone","typical_spend","account_balance","user_age","credit_score"]]
    tx_df = tx_df.join(users_meta, on="user_id")

    print("Computing behavioral features...")
    tx_df = compute_behavioral_features(tx_df)

    print("Assigning fraud labels...")
    tx_df = assign_fraud_labels(tx_df, target_rate=CONFIG["target_fraud_rate"], max_allowed=CONFIG["max_allowed_fraud_rate"])

    cols_order = [
        "transaction_id","user_id","merchant_id",
        "timestamp","transaction_date","transaction_hour",
        "amount","currency","amount_usd",
        "country","city","ip_address","timezone",
        "device_type","os","browser",
        "prev_transaction_count_1h","prev_transaction_count_24h",
        "avg_transaction_amount_7d","total_spent_30d",
        "hours_since_last_transaction","transaction_frequency",
        "merchant_category","merchant_risk_score","is_high_risk_merchant","mcc_code",
        "amount_deviation","velocity_1h","amount_velocity",
        "user_age","user_tenure_days","account_balance","credit_score",
        "fraud_score","is_fraud"
    ]
    cols = [c for c in cols_order if c in tx_df.columns]
    tx_df = tx_df[cols]
    
    return tx_df

if __name__ == "__main__":
    df = run_generation()
    print(df.head())
