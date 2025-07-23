import os
import json
import time
import logging
from pathlib import Path
from kafka import KafkaConsumer
from clients.database_client import Database
from utils.binance_websocket import insert_batched_data
from utils.sql_utils import load_sql

BATCH_INTERVAL_SECONDS = 10.0
consumer = KafkaConsumer(
    "binance-ws-trade",
    "binance-ws-bookTicker",
    "binance-ws-markPrice",
    group_id='binance-websocket',
    bootstrap_servers=os.getenv("BOOTSTRAP_SERVER"),
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s")
db = Database(host='192.168.1.67')

last_batch_time = time.time()
trade_buffer = []
book_ticker_buffer = []
mark_price_buffer = []

for message in consumer:
    topic = message.topic
    data = message.value
    consumed_time = time.time() * 1000

    if topic == 'binance-ws-trade':
        row = (
            data.get('trade_id'),
            data.get('symbol'),
            data.get('price'),
            data.get('quantity'),
            data.get('is_buyer_maker'),
            data.get('event_time'),
            data.get('trade_time'),
            data.get('produced_time'),
            consumed_time,
            round(data.get('produced_time') - data.get('trade_time')),
        )
        trade_buffer.append(row)
    elif topic == 'binance-ws-bookTicker':
        row = (
            data.get('book_update_id'),
            data.get('symbol'),
            data.get('bid_price'),
            data.get('bid_quantity'),
            data.get('ask_price'),
            data.get('ask_quantity'),
            data.get('event_time'),
            data.get('transaction_time'),
            data.get('produced_time'),
            consumed_time,
            round(data.get('produced_time') - data.get('transaction_time'))
        )
        book_ticker_buffer.append(row)
    elif topic == 'binance-ws-markPrice':
        row = (
            data.get('symbol'),
            data.get('mark_price'),
            data.get('index_price'),
            data.get('funding_rate'),
            data.get('event_time'),
            data.get('next_funding_rate_est'),
            data.get('next_funding_time'),
            data.get('produced_time'),
            consumed_time,
            round(data.get('produced_time') - data.get('event_time'))
        )
        mark_price_buffer.append(row)
    else:
        logging.error('No valid topics')
        break

    if time.time() - last_batch_time >= BATCH_INTERVAL_SECONDS:
        if trade_buffer:
            insert_query = load_sql(Path(__file__).resolve().parent.parent / "sql" / "insert" / "binance_ws_trade.sql")
            insert_batched_data(db, trade_buffer, insert_query, 'binance-ws-trade')
            trade_buffer = []
        if book_ticker_buffer:
            insert_query = load_sql(Path(__file__).resolve().parent.parent / "sql" / "insert" / "binance_ws_book_ticker.sql")
            insert_batched_data(db, book_ticker_buffer, insert_query, 'binance-ws-bookTicker')
            book_ticker_buffer = []
        if mark_price_buffer:
            insert_query = load_sql(Path(__file__).resolve().parent.parent / "sql" / "insert" / "binance_ws_mark_price.sql")
            insert_batched_data(db, mark_price_buffer, insert_query, 'binance-ws-markPrice')
            mark_price_buffer = []

        last_batch_time = time.time()
