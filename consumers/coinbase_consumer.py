"""
Coinbase Kafka Consumer
Reads messages from coinbase_ticker and coinbase_matches
topics and saves them to CSV staging files.
"""

import json
import csv
import os
from kafka import KafkaConsumer
from datetime import datetime

# ── File paths ───────────────────────────────────────────
DATA_DIR = '/workspaces/WM3B7/data'
TICKER_FILE = f'{DATA_DIR}/ticker_data.csv'
MATCHES_FILE = f'{DATA_DIR}/matches_data.csv'

# ── Ticker CSV headers ───────────────────────────────────
TICKER_HEADERS = [
    'product_id', 'price', 'open_24h', 'volume_24h',
    'low_24h', 'high_24h', 'best_bid', 'best_ask',
    'time', 'ingested_at'
]

# ── Matches CSV headers ──────────────────────────────────
MATCHES_HEADERS = [
    'trade_id', 'product_id', 'price', 'size',
    'side', 'time', 'ingested_at'
]

def init_csv(filepath, headers):
    """Create CSV file with headers if it doesn't exist."""
    if not os.path.exists(filepath):
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
        print(f"Created {filepath}")

def write_row(filepath, headers, data):
    """Append a row to a CSV file."""
    row = {h: data.get(h, '') for h in headers}
    with open(filepath, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writerow(row)

def main():
    # Initialise CSV files
    init_csv(TICKER_FILE, TICKER_HEADERS)
    init_csv(MATCHES_FILE, MATCHES_HEADERS)

    # Set up Kafka consumer for both topics
    consumer = KafkaConsumer(
        'coinbase_ticker',
        'coinbase_matches',
        bootstrap_servers='localhost:29092',
        auto_offset_reset='earliest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        consumer_timeout_ms=10000  # Stop after 10s of no messages
    )

    print("Reading from Kafka topics...")
    ticker_count = 0
    matches_count = 0

    for msg in consumer:
        topic = msg.topic
        data = msg.value

        if topic == 'coinbase_ticker':
            write_row(TICKER_FILE, TICKER_HEADERS, data)
            ticker_count += 1
            print(f"[TICKER] Saved {data.get('product_id')} - £{data.get('price')}")

        elif topic == 'coinbase_matches':
            write_row(MATCHES_FILE, MATCHES_HEADERS, data)
            matches_count += 1
            print(f"[MATCH]  Saved {data.get('product_id')} - {data.get('size')} @ {data.get('price')}")

    print(f"\n✅ Done! Saved {ticker_count} ticker rows and {matches_count} match rows")
    print(f"📁 Ticker data: {TICKER_FILE}")
    print(f"📁 Matches data: {MATCHES_FILE}")

if __name__ == '__main__':
    main()
