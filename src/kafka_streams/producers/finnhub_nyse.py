import json
import os
import pytz
import finnhub
from kafka import KafkaProducer
from datetime import datetime, timezone
from finnhub.exceptions import FinnhubAPIException
from utils.finnhub_nyse import TOP_STOCKS
from utils.finnhub_nyse import sleep_until_next_minute_plus_10, is_within_work_hours

TOPIC = 'finnhub-nyse-stock-prices'
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")


def fetch_stock_price(symbol):
    finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)
    response = finnhub_client.quote(symbol)
    return response


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Stream to Kafka
while True:
    if is_within_work_hours():
        for category in TOP_STOCKS:
            for stock in TOP_STOCKS[category]:
                try:
                    stock_data = fetch_stock_price(stock)
                except FinnhubAPIException as e:
                    sleep_until_next_minute_plus_10()
                    exit = True
                    break

                stock_time = datetime.fromtimestamp(stock_data['t'], timezone.utc).astimezone(
                    pytz.timezone("America/New_York")).strftime('%Y-%m-%d %H:%M:00')
                stock_data_payload = {
                    'symbol': stock,
                    'category': category,
                    'price': stock_data['c'],
                    'time': stock_time
                }
                producer.send(TOPIC, key=stock.encode('utf-8'), value=stock_data_payload)
                producer.flush()
            if exit:
                exit = False
                break

    sleep_until_next_minute_plus_10()
