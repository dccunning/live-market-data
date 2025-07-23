INSERT INTO crypto.binance_ws_mark_price
    (
        symbol,
        mark_price,
        index_price,
        funding_rate,
        event_time,
        next_funding_rate_est,
        next_funding_time,
        produced_time,
        consumed_time,
        drift
    )
VALUES %s
ON CONFLICT (symbol, event_time)
DO NOTHING;