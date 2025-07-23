import httpx
import asyncio
import logging
from httpx import AsyncClient
from aiokafka import AIOKafkaProducer


logging.getLogger("httpx").setLevel(logging.WARNING)


async def get_data_and_produce(client: AsyncClient, producer: AIOKafkaProducer, topic: str, key: str, url: str):
    """
    Fetches data from a REST API and produces each item to a Kafka topic.

    :param client: HTTP client used to fetch data
    :param producer: Kafka producer instance
    :param topic: Kafka topic to send data to
    :param key: Field name in each result to use as message key
    :param url: API endpoint to fetch data from
    :return: None
    """
    try:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        logging.warning(f"{topic} API error: {e}")
        return

    for result in data:
        try:
            await producer.send_and_wait(
                topic=topic,
                key=str(result.get(key)),
                value=result
            )
        except Exception as e:
            logging.error(f"{topic} Kafka error sending {result.get(key)}: {e}")


async def producer_stream_api_book_price(producer: AIOKafkaProducer, topic: str, key: str, url: str, frequency: float):
    """
    Fetches data from an API and produces it to a Kafka topic.

    :param producer: Kafka producer instance
    :param topic: Kafka topic to send data to
    :param key: Field name in each result to use as message key
    :param url: API endpoint to fetch data from
    :param frequency: Time interval between API calls in seconds
    :return: None
    """
    async with httpx.AsyncClient() as client:
        while True:
            asyncio.create_task(get_data_and_produce(client, producer, topic, key, url))
            await asyncio.sleep(frequency)
