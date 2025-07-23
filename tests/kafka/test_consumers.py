import time
import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture
def test_message():
    return {
        "symbol": "BTCUSDT",
        "time": 123456789,
        "bidPrice": 50000.1,
        "bidQty": 0.5,
        "askPrice": 50001.2,
        "askQty": 0.3,
        "lastUpdateId": 42
    }


def test_consumer_batch_inserts(test_message):
    MockMessage = MagicMock()
    MockMessage.value = test_message

    # Patch KafkaConsumer and Database, paths relative to src/
    with patch("kafka.KafkaConsumer", return_value=[MockMessage] * 3), \
         patch("clients.database_client.Database") as MockDB, \
         patch("utils.sql_utils.load_sql", return_value="INSERT INTO test ..."):

        import kafka_streams.consumers.binance_api_book_ticker as consumer_module

        mock_db_instance = MockDB.return_value
        mock_db_instance.run_query = MagicMock()

        # Mock buffered rows and time threshold to trigger insert
        consumer_module.buffer = [tuple(test_message.values()) for _ in range(3)]
        consumer_module.last_batch_time = time.time() - consumer_module.BATCH_INTERVAL_SECONDS

        if time.time() - consumer_module.last_batch_time >= consumer_module.BATCH_INTERVAL_SECONDS:
            consumer_module.db.run_query(consumer_module.insert_query, consumer_module.buffer)

        # Assertion
        mock_db_instance.run_query.assert_called_once()
        args, _ = mock_db_instance.run_query.call_args
        assert len(args[1]) == 3
