CREATE TABLE crypto.binance_api_price (
    symbol TEXT NOT NULL,
    time BIGINT NOT NULL,
    price NUMERIC,
    UNIQUE (symbol, time)
);