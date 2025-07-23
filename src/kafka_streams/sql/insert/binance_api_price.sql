INSERT INTO crypto.binance_api_price
(symbol, time, price)
VALUES %s
ON CONFLICT (symbol, time) DO NOTHING;