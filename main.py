import os
import random
from datetime import datetime, timezone

import faker
import psycopg2

fake = faker.Faker()

NUM_RECORDS = int(os.getenv("NUM_RECORDS", "1000"))


def generate_transaction():
    user = fake.simple_profile()

    return {
        "transactionId": fake.uuid4(),
        "userId": user["username"],
        # naive UTC datetime, matches the TIMESTAMP (no time zone) column
        "timestamp": datetime.now(timezone.utc).replace(tzinfo=None),
        "amount": round(random.uniform(10, 1000), 2),
        "currency": random.choice(["USD", "GBP"]),
        "city": fake.city(),
        "country": fake.country(),
        "merchantName": fake.company(),
        "paymentMethod": random.choice(["credit_card", "debit_card", "online_transfer"]),
        "ipAddress": fake.ipv4(),
        "voucherCode": random.choice(["", "DISCOUNT10", ""]),
        "affiliateId": fake.uuid4(),
    }


def create_table(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id VARCHAR(255) PRIMARY KEY,
                user_id VARCHAR(255),
                timestamp TIMESTAMP,
                amount DECIMAL,
                currency VARCHAR(255),
                city VARCHAR(255),
                country VARCHAR(255),
                merchant_name VARCHAR(255),
                payment_method VARCHAR(255),
                ip_address VARCHAR(255),
                voucher_code VARCHAR(255),
                affiliate_id VARCHAR(255)
            )
            """
        )
    conn.commit()


if __name__ == "__main__":

    conn = psycopg2.connect(
        host="localhost",
        database="financial_db",
        user="postgres",
        password=6112001,
        port=5432,
    )

    try:
        create_table(conn)

        print(f"Starting to insert {NUM_RECORDS} records...")
        with conn.cursor() as cur:
            for i in range(1, NUM_RECORDS + 1):
                transaction = generate_transaction()

                cur.execute(
                    """
                    INSERT INTO transactions (
                        transaction_id, user_id, timestamp, amount, currency,
                        city, country, merchant_name, payment_method,
                        ip_address, voucher_code, affiliate_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        transaction["transactionId"],
                        transaction["userId"],
                        transaction["timestamp"],
                        transaction["amount"],
                        transaction["currency"],
                        transaction["city"],
                        transaction["country"],
                        transaction["merchantName"],
                        transaction["paymentMethod"],
                        transaction["ipAddress"],
                        transaction["voucherCode"],
                        transaction["affiliateId"],
                    ),
                )

                if i % 100 == 0:
                    conn.commit()
                    print(f"  Inserted {i}/{NUM_RECORDS} records...")

        conn.commit()
        print(f"Done! {NUM_RECORDS} records inserted successfully.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()