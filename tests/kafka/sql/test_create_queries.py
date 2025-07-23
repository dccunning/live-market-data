import sqlite3
import pytest
from pathlib import Path
from utils.sql_utils import load_sql


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


@pytest.mark.parametrize("filename", [
    "binance_api_book_ticker.sql",
    "binance_api_price.sql",
    "binance_ws_book_ticker.sql",
    "binance_ws_mark_price.sql",
])
def test_create_table_sql_syntax(db, filename):
    """
    Asserts that each CREATE TABLE SQL runs without syntax errors.
    """
    sql_path = Path(__file__).resolve().parents[3] / "src" / "kafka_streams" / "sql" / "create" / filename
    create_sql = load_sql(sql_path).replace("crypto.", "")

    cursor = db.cursor()
    cursor.executescript(create_sql)  # Raises error if SQL is invalid

    assert True
