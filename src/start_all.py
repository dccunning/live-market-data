import subprocess
import threading
import logging
import socket
import glob
import time
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s")
BASE_DIR = os.path.dirname(__file__)
IGNORE_STREAMS = [
    'finnhub_nyse.py',
    'binance_websocket.py'
]


def wait_for_kafka(host="localhost", port=9092, timeout=90):
    logging.info(f"⏳ Waiting for Kafka at {host}:{port}...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                logging.info("✅ Kafka is available!")
                return
        except OSError:
            time.sleep(1)
    raise TimeoutError("❌ Kafka not available after waiting.")


def run_script_with_retry(script_path, retry_delay=5):
    while True:
        relative = os.path.relpath(script_path, BASE_DIR)
        logging.info(f"🚀 Starting {relative}")

        proc = subprocess.Popen(["python3", script_path])
        exit_code = proc.wait()
        if exit_code == 0:
            logging.info(f"{script_path} exited normally.")
            break
        else:
            logging.error(f"{script_path} exited with code {exit_code}. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)


def run_all(relative_path):
    abs_path = os.path.join(BASE_DIR, relative_path)
    scripts = glob.glob(f"{abs_path}/*.py")
    threads = []
    for script in scripts:
        if script.split("/")[-1] in IGNORE_STREAMS:
            logging.info(f"⏭️ Skipping {relative_path}/{script.split("/")[-1]}")
            continue
        else:
            t = threading.Thread(target=run_script_with_retry, args=(script,))
            t.start()
            threads.append(t)

    return threads


if __name__ == "__main__":
    wait_for_kafka()

    threads = []
    threads.extend(run_all("producers"))
    threads.extend(run_all("consumers"))

    for t in threads:
        t.join()
