INSERT INTO crypto.binance_api_book_ticker
(symbol, time, bidPrice, bidQty, askPrice, askQty, lastUpdateId)
VALUES %s
ON CONFLICT (symbol, time) DO NOTHING;