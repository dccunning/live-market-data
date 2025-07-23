CREATE TABLE crypto.binance_api_book_ticker (
    symbol TEXT NOT NULL,
    time BIGINT NOT NULL,
    bidPrice NUMERIC,
    bidQty NUMERIC,
    askPrice NUMERIC,
    askQty NUMERIC,
    lastUpdateId BIGINT,
    UNIQUE (symbol, time)
);