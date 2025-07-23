INSERT INTO crypto.binance_ws_book_ticker
    (
        book_update_id,
        symbol,
        bid_price,
        bid_quantity,
        ask_price,
        ask_quantity,
        event_time,
        transaction_time,
        produced_time,
        consumed_time,
        drift
    )
VALUES %s
ON CONFLICT (symbol, book_update_id)
DO NOTHING;