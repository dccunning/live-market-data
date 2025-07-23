import os
import json
import time
import logging
from pathlib import Path
from kafka import KafkaConsumer
from clients.database_client import Database
from utils.sql_utils import load_sql

TOPIC = 'binance-api-price'
GROUP = 'binance-api'
BATCH_INTERVAL_SECONDS = 10.0

consumer = KafkaConsumer(
    TOPIC,
    group_id=GROUP,
    bootstrap_servers=os.getenv("BOOTSTRAP_SERVER"),
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s")
db = Database(host='192.168.1.67')

insert_query = load_sql(Path(__file__).resolve().parent.parent / "sql" / "insert" / "binance_api_price.sql")

last_batch_time = time.time()
buffer = []

for message in consumer:
    data = message.value
    consumed_time = time.time() * 1000

    row = (
        data.get('symbol'),
        data.get('time'),
        data.get('price')
    )
    buffer.append(row)

    if time.time() - last_batch_time >= BATCH_INTERVAL_SECONDS:
        if buffer:
            try:
                db.run_query(insert_query, buffer)
                logging.log(logging.INFO, f"Inserted {len(buffer):>5} rows")
                buffer = []
            except Exception as e:
                logging.warning(f"Insert query failed: {e}")

        last_batch_time = time.time()
