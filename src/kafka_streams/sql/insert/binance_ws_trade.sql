INSERT INTO crypto.binance_ws_trade
    (
        trade_id,
        symbol,
        price,
        quantity,
        is_buyer_maker,
        event_time,
        trade_time,
        produced_time,
        consumed_time,
        drift
    )
VALUES %s
ON CONFLICT (symbol, trade_id)
DO NOTHING;