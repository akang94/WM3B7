"""
Coinbase WebSocket Producer
Connects to Coinbase real-time feed and publishes
ticker + matches data to Kafka topics.
"""

import asyncio
import json
import websockets
from kafka import KafkaProducer
from datetime import datetime

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Coinbase WebSocket URL (no auth needed)
WS_URL = 'wss://ws-feed.exchange.coinbase.com'

# Products and channels to subscribe to
SUBSCRIBE_MSG = {
    "type": "subscribe",
    "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD"],
    "channels": ["ticker", "matches"]
}

async def stream_coinbase():
    print("Connecting to Coinbase WebSocket...")
    async with websockets.connect(WS_URL) as ws:
        # Send subscription message
        await ws.send(json.dumps(SUBSCRIBE_MSG))
        print("Subscribed to BTC-USD, ETH-USD, SOL-USD")
        print("Streaming data into Kafka... (Ctrl+C to stop)\n")

        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            # Add ingestion timestamp
            data['ingested_at'] = datetime.utcnow().isoformat()

            # Route to correct Kafka topic based on message type
            if data.get('type') == 'ticker':
                producer.send('coinbase_ticker', value=data)
                print(f"[TICKER] {data.get('product_id')} - Price: {data.get('price')}")

            elif data.get('type') == 'match':
                producer.send('coinbase_matches', value=data)
                print(f"[MATCH]  {data.get('product_id')} - Size: {data.get('size')} @ {data.get('price')}")

            producer.flush()

if __name__ == '__main__':
    asyncio.run(stream_coinbase())
