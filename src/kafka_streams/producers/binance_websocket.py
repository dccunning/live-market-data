import os
import json
import logging
import asyncio
from aiokafka import AIOKafkaProducer
from utils.binance_websocket import websocket_producer_stream

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s")

TRADE_ID_KEY = 'symbol'
COINS = ['btc', 'eth', 'xmr']
STREAMS = {
    "trade": {
        "trade_id": "t",
        "symbol": "s",
        "price": "p",
        "quantity": "q",
        "is_buyer_maker": "m",
        "event_time": "E",
        "trade_time": "T"
    },
    "bookTicker": {
        "book_update_id": "u",
        "symbol": "s",
        "bid_price": "b",
        "bid_quantity": "B",
        "ask_price": "a",
        "ask_quantity": "A",
        "event_time": "E",
        "transaction_time": "T"
    },
    "markPrice": {
        "symbol": "s",
        "mark_price": "p",
        "index_price": "i",
        "funding_rate": "P",
        "event_time": "E",
        "next_funding_rate_est": "r",
        "next_funding_time": "T"
    }
}
WS_URL = (
    "wss://fstream.binance.com/stream?streams=" +
    "/".join(f"{c}usdt@{s}" for c in COINS for s in STREAMS.keys())
)

_topics = """ binance-ws-trade, binance-ws-bookTicker, binance-ws-markPrice
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic TOPIC \
  --partitions 12 \
  --replication-factor 1 \
  --config segment.bytes=1048576 \
  --config retention.bytes=8388608 \
  --config retention.ms=86400000 \
  --config cleanup.policy=delete

kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic TOPIC
"""


async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=os.getenv("BOOTSTRAP_SERVER"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8")
    )
    await producer.start()
    logging.log(logging.INFO, f"Started producer")
    try:
        await websocket_producer_stream(
            url=WS_URL,
            response_mapping=STREAMS,
            producer=producer,
            key=TRADE_ID_KEY
        )
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
