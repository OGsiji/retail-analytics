"""
Sample data generator for testing the churn prediction pipeline
Use this if you don't have the actual datasets
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
import uuid


def generate_users_data(num_users=1000):
    """Generate sample users data"""

    regions = ["lagos", "abuja", "kano", "port_harcourt", "ibadan", "benin", "jos"]
    channels = ["organic", "paid", "social", "referral", "email"]

    users = []
    for i in range(1, num_users + 1):
        signup_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 600))

        users.append(
            {
                "user_id": i,
                "signup_date": signup_date.strftime("%Y-%m-%d %H:%M:%S"),
                "region": random.choice(regions),
                "channel": random.choice(channels),
            }
        )

    df = pd.DataFrame(users)
    df.to_csv("include/datasets/users.csv", index=False)
    print(f"✅ Generated {num_users} users in users.csv")

    return df


def generate_user_activities_data(users_df, events_per_user=30):
    """Generate sample user activities data"""

    event_names = [
        "session_start",
        "page_view",
        "add_to_cart",
        "purchase",
        "session_end",
    ]
    devices = ["phone", "tablet", "desktop"]
    app_versions = [
        "1.0.6",
        "1.2.9",
        "1.5.0",
        "1.5.5",
        "1.7.7",
        "2.0.5",
        "2.3.3",
        "2.3.7",
        "2.4.6",
        "2.5.0",
        "2.8.0",
        "3.1.9",
        "3.2.0",
        "3.4.5",
        "3.8.9",
    ]

    activities = []

    for _, user in users_df.iterrows():
        user_id = user["user_id"]
        signup_date = datetime.strptime(user["signup_date"], "%Y-%m-%d %H:%M:%S")

        # Generate random number of activities for this user
        num_activities = random.randint(1, events_per_user)

        for _ in range(num_activities):
            # Generate random activity date after signup
            days_after_signup = random.randint(0, 400)
            activity_date = signup_date + timedelta(
                days=days_after_signup,
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )

            # Don't generate activities too far in the future
            if activity_date > datetime.now():
                continue

            activities.append(
                {
                    "user_id": user_id,
                    "session_id": str(uuid.uuid4()),
                    "event_name": random.choice(event_names),
                    "event_timestamp": activity_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "device": random.choice(devices),
                    "app_version": random.choice(app_versions),
                }
            )

    df = pd.DataFrame(activities)

    # Save as CSV
    df.to_csv("include/datasets/user_activities.csv", index=False)

    # Save as JSONL
    with open("include/datasets/user_activities.jsonl", "w") as f:
        for _, row in df.iterrows():
            f.write(json.dumps(row.to_dict()) + "\n")

    print(
        f"✅ Generated {len(activities)} activities in user_activities.csv and user_activities.jsonl"
    )
    return df


def generate_transactions_sql(users_df, transactions_per_user=10):
    """Generate sample transactions SQL dump"""

    statuses = ["success", "failed", "refunded"]
    status_weights = [0.9, 0.07, 0.03]  # 90% success, 7% failed, 3% refunded

    sql_lines = [
        "-- PostgreSQL transactions dump",
        "DROP TABLE IF EXISTS transactions;",
        "CREATE TABLE transactions (",
        "    id SERIAL PRIMARY KEY,",
        "    user_id INTEGER NOT NULL,",
        "    amount DECIMAL(10,2) NOT NULL,",
        "    currency VARCHAR(3) DEFAULT 'NGN',",
        "    status VARCHAR(20) NOT NULL,",
        "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        ");",
        "",
        "INSERT INTO transactions (user_id, amount, currency, status, created_at) VALUES",
    ]

    transaction_values = []
    transaction_id = 1

    for _, user in users_df.iterrows():
        user_id = user["user_id"]
        signup_date = datetime.strptime(user["signup_date"], "%Y-%m-%d %H:%M:%S")

        # Generate random number of transactions
        num_transactions = random.randint(0, transactions_per_user)

        for _ in range(num_transactions):
            # Generate transaction date after signup
            days_after_signup = random.randint(1, 400)
            transaction_date = signup_date + timedelta(
                days=days_after_signup,
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )

            # Don't generate transactions in the future
            if transaction_date > datetime.now():
                continue

            # Generate amount (skewed towards smaller amounts)
            if random.random() < 0.7:  # 70% small amounts
                amount = round(random.uniform(100, 5000), 2)
            elif random.random() < 0.25:  # 25% medium amounts
                amount = round(random.uniform(5000, 50000), 2)
            else:  # 5% large amounts
                amount = round(random.uniform(50000, 500000), 2)

            status = random.choices(statuses, weights=status_weights)[0]

            transaction_values.append(
                f"({user_id}, {amount}, 'NGN', '{status}', '{transaction_date.strftime('%Y-%m-%d %H:%M:%S')}')"
            )
            transaction_id += 1

    # Add values to SQL
    sql_lines.extend(
        [
            ",\n".join(transaction_values) + ";",
            "",
            "-- Create indexes for better performance",
            "CREATE INDEX idx_transactions_user_id ON transactions(user_id);",
            "CREATE INDEX idx_transactions_status ON transactions(status);",
            "CREATE INDEX idx_transactions_created_at ON transactions(created_at);",
            "",
        ]
    )

    # Write SQL file
    with open("include/datasets/postgres_transactions_dump.sql", "w") as f:
        f.write("\n".join(sql_lines))

    print(
        f"✅ Generated {len(transaction_values)} transactions in postgres_transactions_dump.sql"
    )


def generate_all_sample_data():
    """Generate sample data matching the actual structure"""
    print("🎲 Generating sample data for churn prediction pipeline...")
    print("=" * 60)

    # Create datasets directory if it doesn't exist
    import os

    os.makedirs("include/datasets", exist_ok=True)

    # Generate users and transactions SQL dump (matches actual structure)
    generate_users_and_transactions_sql()

    # Generate user activities CSV
    generate_user_activities_csv()

    print("\n🎉 Sample data generation complete!")
    print("\nGenerated files:")
    print("  • include/datasets/user_activities.csv")
    print(
        "  • include/datasets/postgres_transactions_dump.sql (contains both users and transactions)"
    )
    print("\nYou can now run the pipeline with this sample data!")


def generate_users_and_transactions_sql():
    """Generate SQL dump with both users and transactions tables"""
    regions = [
        "Lagos",
        "Abuja",
        "Kano",
        "Oyo",
        "Anambra",
        "Enugu",
        "Edo",
        "Rivers",
        "Ogun",
    ]
    channels = ["web", "android", "ios"]
    statuses = ["success", "failed", "refunded"]
    status_weights = [0.9, 0.07, 0.03]

    sql_lines = [
        "-- PostgreSQL dump for churn assessment",
        "BEGIN;",
        "",
        "DROP TABLE IF EXISTS transactions;",
        "DROP TABLE IF EXISTS users;",
        "",
        "CREATE TABLE users (",
        "   user_id INT PRIMARY KEY,",
        "   email VARCHAR(255),",
        "   region VARCHAR(100),",
        "   signup_channel VARCHAR(50),",
        "   created_at TIMESTAMP",
        ");",
        "",
        "CREATE TABLE transactions (",
        "   transaction_id UUID PRIMARY KEY,",
        "   user_id INT REFERENCES users(user_id),",
        "   amount NUMERIC(12,2),",
        "   currency VARCHAR(10),",
        "   status VARCHAR(20),",
        "   created_at TIMESTAMP",
        ");",
        "",
    ]

    # Generate users
    user_inserts = []
    for i in range(1, 801):  # 800 users
        signup_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 600))
        user_inserts.append(
            f"({i}, 'user{i}@example.com', '{random.choice(regions)}', "
            f"'{random.choice(channels)}', '{signup_date.strftime('%Y-%m-%d %H:%M:%S')}')"
        )

    sql_lines.append(
        "INSERT INTO users (user_id, email, region, signup_channel, created_at) VALUES"
    )
    sql_lines.append(",\n".join(user_inserts) + ";")
    sql_lines.append("")

    # Generate transactions
    transaction_inserts = []
    for user_id in range(1, 801):
        # Random number of transactions per user
        num_transactions = random.randint(0, 15)

        for _ in range(num_transactions):
            transaction_date = datetime(2024, 1, 1) + timedelta(
                days=random.randint(1, 600), hours=random.randint(0, 23)
            )

            if transaction_date > datetime.now():
                continue

            # Generate realistic amounts
            if random.random() < 0.7:
                amount = round(random.uniform(100, 5000), 2)
            elif random.random() < 0.25:
                amount = round(random.uniform(5000, 50000), 2)
            else:
                amount = round(random.uniform(50000, 500000), 2)

            status = random.choices(statuses, weights=status_weights)[0]
            transaction_id = str(uuid.uuid4())

            transaction_inserts.append(
                f"('{transaction_id}', {user_id}, {amount}, 'NGN', '{status}', "
                f"'{transaction_date.strftime('%Y-%m-%d %H:%M:%S')}')"
            )

    if transaction_inserts:
        sql_lines.append(
            "INSERT INTO transactions (transaction_id, user_id, amount, currency, status, created_at) VALUES"
        )
        sql_lines.append(",\n".join(transaction_inserts) + ";")

    sql_lines.extend(["", "COMMIT;", ""])

    # Write SQL file
    with open("include/datasets/postgres_transactions_dump.sql", "w") as f:
        f.write("\n".join(sql_lines))

    print(
        f"✅ Generated users and {len(transaction_inserts)} transactions in postgres_transactions_dump.sql"
    )


def generate_user_activities_csv():
    """Generate user activities CSV file"""
    event_names = [
        "session_start",
        "page_view",
        "add_to_cart",
        "purchase",
        "session_end",
    ]
    devices = ["phone", "tablet", "desktop"]
    app_versions = [
        "1.0.6",
        "1.2.9",
        "1.5.0",
        "1.5.5",
        "2.0.5",
        "2.3.3",
        "2.8.0",
        "3.1.9",
    ]

    activities = []

    for user_id in range(1, 801):  # Match the users in SQL dump
        # Random number of activities
        num_activities = random.randint(5, 50)

        for _ in range(num_activities):
            activity_date = datetime(2024, 1, 1) + timedelta(
                days=random.randint(0, 600),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            )

            if activity_date > datetime.now():
                continue

            activities.append(
                {
                    "user_id": user_id,
                    "session_id": str(uuid.uuid4()),
                    "event_name": random.choice(event_names),
                    "event_timestamp": activity_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "device": random.choice(devices),
                    "app_version": random.choice(app_versions),
                }
            )

    df = pd.DataFrame(activities)
    df.to_csv("include/datasets/user_activities.csv", index=False)

    print(f"✅ Generated {len(activities)} activities in user_activities.csv")


if __name__ == "__main__":
    generate_all_sample_data()
