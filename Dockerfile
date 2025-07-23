FROM python:3.12-slim

WORKDIR /

COPY trading_data/streaming/kafka/ ./kafka
COPY clients/ ./clients
COPY utils/ ./utils

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH="/clients:/utils:$PYTHONPATH"

# Start Kafka producer and consumer scripts
CMD ["python", "kafka/start_all.py"]