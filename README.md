# Market Feed Streaming Pipeline

A real-time streaming system with producer and consumer pipelines for ingesting 
and processing crypto and equity market data. Kafka streams are hosted in Docker 
containers running on a home server, enabling easy access for other trading related
projects to consume the processed data.

## Features

- 50ms latency data ingestion from Binance
- Kafka based low-latency streaming
- SQL schema for structured storage
- Dockerized infrastructure (Kafka, Zookeeper, Kadeck)

---

## Project Structure

```
src/
├── start_all.py          # Main entry point to activate all producers & consumers
├── clients/              # External data clients (ex. Database client)
├── utils/                # Shared utilities
└── kafka/
    ├── consumers/        # Kafka consumers for processing messages
    ├── producers/        # Kafka producers for raw data ingestion
    └── sql/              # SQL schema and insertion logic
```

---

## Kafka

### Producers
1. **Binance Book Ticker** – Streams bid/ask levels
2. **Binance Trade Prices** – Streams latest trade prices
3. **Binance WebSocket Feed** – High-frequency tick data
4. **Finnhub NYSE Quotes** – Real-time NYSE equity quote data

### Consumers
- Process and normalize incoming data
- Store selected data in SQL database to support signal generation and backtesting engines
- Consume in real-time from other projects to support live algorithmic trading systems

---

## Instructions
1. Run the Dockerfile to execute start_all.py
2. Monitor the logs for real-time frequency delays
