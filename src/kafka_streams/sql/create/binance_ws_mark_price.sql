CREATE TABLE crypto.binance_ws_mark_price (
    symbol TEXT NOT NULL,
    mark_price NUMERIC,
    index_price NUMERIC,
    funding_rate NUMERIC,
    drift NUMERIC,
    consumed_time NUMERIC,
    produced_time NUMERIC,
    event_time BIGINT,
    next_funding_rate_est NUMERIC,
    next_funding_time BIGINT,
    PRIMARY KEY (symbol, event_time)
);