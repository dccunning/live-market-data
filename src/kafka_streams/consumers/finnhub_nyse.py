import logging
import os
import json
import psycopg2
from kafka import KafkaConsumer

TOPIC = 'finnhub-nyse-stock-prices'
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=os.getenv('BOOTSTRAP_SERVER'),
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

conn = psycopg2.connect(
    dbname=os.getenv("DATABASE_NAME"),
    user=os.getenv("DATABASE_USER"),
    password=os.getenv("DATABASE_PASSWORD"),
    host=os.getenv("DATABASE_HOME_HOST"),
    port=os.getenv("DATABASE_PORT")
)
cur = conn.cursor()

for message in consumer:
    try:
        stock_data = message.value
        symbol = stock_data['symbol']
        category = stock_data['category']
        price = stock_data['price']
        timestamp = stock_data['time']

        cur.execute("""
            INSERT INTO stocks.finnhub_data (symbol, category, price, time)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (symbol, time) DO NOTHING;
        """, (symbol, category, price, timestamp))

        conn.commit()
    except Exception as e:
        logging.log(logging.INFO, f"Error: {e}")

cur.close()
conn.close()

