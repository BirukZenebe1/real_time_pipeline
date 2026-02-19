import json
import os
import time

import psycopg2
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "bolt_rides")
RUN_ONCE = os.getenv("RUN_ONCE", "false").lower() == "true"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "rides_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")

# Defaults match local Docker; set env vars for AWS endpoints.
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

conn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
)
cursor = conn.cursor()

def fetch_rides():
    cursor.execute("SELECT * FROM rides ORDER BY ride_id;")
    return cursor.fetchall()

def main():
    last_sent_id = 0
    while True:
        cursor.execute(
            "SELECT * FROM rides WHERE ride_id > %s ORDER BY ride_id;",
            (last_sent_id,),
        )
        rides = cursor.fetchall()

        if not rides:
            if RUN_ONCE:
                print("No new rides found. RUN_ONCE=true, exiting.")
                break
            time.sleep(1)
            continue

        for ride in rides:
            ride_event = {
                "ride_id": ride[0],
                "driver_id": ride[1],
                "city": ride[2],
                "timestamp": str(ride[3]),
                "price": float(ride[4]),
                "status": ride[5]
            }
            producer.send(KAFKA_TOPIC, ride_event)
            last_sent_id = ride[0]
            print(f"Produced: {ride_event}")
            time.sleep(1)  # simulate real-time events

        if RUN_ONCE:
            print("Processed available rides once. RUN_ONCE=true, exiting.")
            break

if __name__ == "__main__":
    main()
