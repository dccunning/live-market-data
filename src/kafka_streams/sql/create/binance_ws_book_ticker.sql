CREATE TABLE crypto.binance_ws_book_ticker (
    book_update_id BIGINT NOT NULL,
    symbol TEXT NOT NULL,
    bid_price NUMERIC,
    bid_quantity NUMERIC,
    ask_price NUMERIC,
    ask_quantity NUMERIC,
    drift NUMERIC,
    consumed_time NUMERIC,
    produced_time NUMERIC,
    event_time BIGINT,
    transaction_time BIGINT,
    PRIMARY KEY (symbol, book_update_id)
);