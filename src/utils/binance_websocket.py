import ssl
import json
import time
import asyncio
import logging
import websockets
from typing import List
from collections import deque
from aiokafka import AIOKafkaProducer
from src.clients.database_client import Database

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE
semaphore = asyncio.Semaphore(10)


async def websocket_producer_stream(
        url: str,
        response_mapping: dict,
        producer: AIOKafkaProducer,
        key: str,
        topic: str = None
):
    """
    Consumes messages from a WebSocket, maps and enriches them and produces them to Kafka.

    :param url: WebSocket endpoint to connect to
    :param response_mapping: Mapping of Kafka field names to source field names
    :param producer: Kafka producer instance
    :param key: Field name to use as Kafka message key
    :param topic: Kafka topic to send messages to
    :return: None
    """
    fallback_buffer = deque(maxlen=10000)
    asyncio.create_task(retry_fallback_buffer(producer, fallback_buffer))
    ws_retry_wait = 1

    while True:
        try:
            async with websockets.connect(url, ssl=ssl_context) as ws:
                logging.info(f"WebSocket connection established")
                ws_retry_wait = 1
                async for msg in ws:
                    trade = json.loads(msg)
                    if 'stream' in trade:  # multi-stream
                        stream_type = trade['stream'].split('@')[-1]
                        topic = f'binance-ws-{stream_type}'
                        trade = trade['data']
                        trade_data = {
                            key: trade[value] for key, value in response_mapping[stream_type].items()
                        }
                    else:  # single-stream
                        trade_data = {
                            key: trade[value] for key, value in response_mapping.items()
                        }

                    trade_data["produced_time"] = time.time() * 1000
                    symbol = str(trade_data.get(key, 'unknown'))

                    asyncio.create_task(send_trade_to_kafka(producer, topic, symbol, trade_data, fallback_buffer))

        except Exception as ws_error:
            logging.error(f"WebSocket error: {ws_error}. Reconnecting in 5 seconds...")
            await asyncio.sleep(ws_retry_wait)
            ws_retry_wait = min(ws_retry_wait + 2, 30)


async def send_trade_to_kafka(producer, topic, symbol, trade_data, fallback_buffer):
    """
    Sends a single trade message to Kafka with fallback buffering on failure.

    :param producer: Kafka producer instance
    :param topic: Kafka topic to send the message to
    :param symbol: Kafka key to associate with the message
    :param trade_data: The message payload sent
    :param fallback_buffer: Deque to store failed messages for retry
    :return: None
    """
    async with semaphore:
        try:
            await producer.send_and_wait(
                topic=topic,
                key=symbol,
                value=trade_data
            )
        except Exception as e:
            logging.error(f"{topic}: Kafka error: {e}")
            fallback_buffer.append((topic, symbol, trade_data))


async def retry_fallback_buffer(producer, fallback_buffer):
    """
    Continuously retries sending failed messages from the fallback buffer to Kafka.

    :param producer: Kafka producer instance
    :param fallback_buffer: Deque of failed messages to retry
    :return: None
    """
    while True:
        if fallback_buffer:
            logging.info(f"Retrying {len(fallback_buffer)} buffered trades...")
            retry_success = []
            for topic, symbol, trade in list(fallback_buffer):
                try:
                    await producer.send_and_wait(topic=topic, key=symbol, value=trade)
                    retry_success.append((topic, symbol, trade))
                except Exception as retry_err:
                    logging.error(f"{topic}: Retry failed: {retry_err}")
                    break  # if Kafka is still down, stop retrying and wait 1s

            for item in retry_success:
                fallback_buffer.remove(item)

        await asyncio.sleep(1)


def insert_batched_data(db: Database, data: List[tuple], insert_query: str, topic: str):
    """
    Inserts batched data into the database and logs drift statistics.

    :param db: Database client instance
    :param data: List of data tuples to insert
    :param insert_query: SQL insert query string
    :param topic: Kafka topic name for logging context
    :return: None
    """
    try:
        if data:
            db.run_query(insert_query, data)
        drifts = [r[-1] for r in data]
        drift_stats = {
            "max": round(max(drifts)),
            "avg": round(sum(drifts) / len(drifts)),
            "p95": sorted(drifts)[int(len(drifts) * 0.95) - 1]
        }
        drift_stats_str = f"{{'max': {drift_stats['max']:>4}, 'avg': {drift_stats['avg']:>4}, 'p95': {drift_stats['p95']:>4}}}"
        logging.log(logging.INFO, f"Inserted {len(data):>5} rows - drift: {drift_stats_str} ({topic})")

        if drift_stats['avg'] > 1000:
            logging.warning(f"Average drift is over 1s - drift: {drift_stats_str} ({topic})")

    except Exception as e:
        logging.error(f"Insert query failed: {e}")
