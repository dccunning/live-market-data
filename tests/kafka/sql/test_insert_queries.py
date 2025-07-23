import re
import pytest
import sqlite3
from pathlib import Path
from utils.sql_utils import load_sql


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


def load_sql_safe(path: Path, placeholder_count: int) -> str:
    sql = load_sql(path)
    sql = sql.replace("crypto.", "")  # remove schema
    sql = sql.replace("%s", f"({', '.join(['?'] * placeholder_count)})")  # mock placeholders

    sql = re.sub(r"ON CONFLICT\s*\([^)]+\)\s*DO NOTHING;?", "", sql, flags=re.IGNORECASE)
    return sql


@pytest.mark.parametrize("filename,table,columns,values", [
    (
        "binance_api_book_ticker.sql",
        "binance_api_book_ticker",
        """
        symbol TEXT, time BIGINT, bidPrice NUMERIC, bidQty NUMERIC,
        askPrice NUMERIC, askQty NUMERIC, lastUpdateId BIGINT
        """,
        ("BTCUSDT", 1, 1.0, 1.0, 1.0, 1.0, 123456)
    ),
    (
        "binance_api_price.sql",
        "binance_api_price",
        "symbol TEXT, time BIGINT, price NUMERIC",
        ("BTCUSDT", 1, 12345.67)
    ),
    (
        "binance_ws_book_ticker.sql",
        "binance_ws_book_ticker",
        """
        book_update_id BIGINT, symbol TEXT, bid_price NUMERIC, bid_quantity NUMERIC,
        ask_price NUMERIC, ask_quantity NUMERIC, event_time BIGINT, transaction_time BIGINT,
        produced_time NUMERIC, consumed_time NUMERIC, drift NUMERIC
        """,
        (1, "BTCUSDT", 1.0, 1.0, 1.0, 1.0, 1000, 1010, 1020.5, 1030.5, 5.0)
    ),
    (
        "binance_ws_mark_price.sql",
        "binance_ws_mark_price",
        """
        symbol TEXT, mark_price NUMERIC, index_price NUMERIC, funding_rate NUMERIC,
        event_time BIGINT, next_funding_rate_est NUMERIC, next_funding_time BIGINT,
        produced_time NUMERIC, consumed_time NUMERIC, drift NUMERIC
        """,
        ("BTCUSDT", 1.0, 1.0, 0.001, 1000, 0.002, 2000, 1020.5, 1030.5, 5.0)
    ),
    (
        "binance_ws_trade.sql",
        "binance_ws_trade",
        """
        trade_id BIGINT, symbol TEXT, price NUMERIC, quantity NUMERIC, is_buyer_maker BOOLEAN,
        event_time BIGINT, trade_time BIGINT, produced_time NUMERIC, consumed_time NUMERIC, drift NUMERIC
        """,
        (123, "BTCUSDT", 1.0, 0.5, True, 1000, 1010, 1020.5, 1030.5, 5.0)
    ),
])
def test_insert_sql_syntax(db, filename, table, columns, values):
    """
    Asserts that each INSERT SQL runs without syntax errors in SQLite.
    """
    sql_path = Path(__file__).resolve().parents[3] / "src" / "kafka_streams" / "sql" / "insert" / filename
    insert_sql = load_sql_safe(sql_path, len(values))

    cursor = db.cursor()
    cursor.execute(f"CREATE TABLE {table} ({columns})")
    cursor.execute(insert_sql, values)

    assert True
