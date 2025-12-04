#!/usr/bin/env python3
"""
Simple Cloud Test Server
Simulates a Cloud API that serves messages to downstream brokers via /pipe/poll
Now generates continuous stream of price updates
"""

from flask import Flask, jsonify, request
from datetime import datetime
import json
import time
import random
import threading

app = Flask(__name__)

# Simulated message storage - will grow dynamically
messages = []

# Stock symbols to generate data for
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "META", "NVDA", "NFLX", "AMD", "INTC"]

# Base prices for stocks
BASE_PRICES = {
    "AAPL": 150.25,
    "GOOGL": 2800.50,
    "MSFT": 380.75,
    "TSLA": 245.30,
    "AMZN": 178.90,
    "META": 485.20,
    "NVDA": 875.45,
    "NFLX": 625.30,
    "AMD": 165.80,
    "INTC": 42.15
}

# Current prices (will fluctuate)
current_prices = BASE_PRICES.copy()

# Lock for thread-safe message append
messages_lock = threading.Lock()

def generate_price_update():
    """Generate a new price update message"""
    symbol = random.choice(SYMBOLS)

    # Price fluctuation: +/- 2%
    fluctuation = random.uniform(-0.02, 0.02)
    current_prices[symbol] *= (1 + fluctuation)

    # Keep price within reasonable bounds
    if current_prices[symbol] < BASE_PRICES[symbol] * 0.7:
        current_prices[symbol] = BASE_PRICES[symbol] * 0.7
    elif current_prices[symbol] > BASE_PRICES[symbol] * 1.3:
        current_prices[symbol] = BASE_PRICES[symbol] * 1.3

    message = {
        "msgKey": f"{symbol}_{len(messages)}",
        "eventType": "MESSAGE",
        "topic": "price-topic",  # Topic for routing
        "data": json.dumps({
            "symbol": symbol,
            "price": round(current_prices[symbol], 2),
            "volume": random.randint(1000, 100000),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }),
        "createdAt": datetime.utcnow().isoformat() + "Z"
    }

    return message

def message_generator():
    """Background thread to continuously generate messages"""
    print("[GENERATOR] Starting message generator thread...")

    while True:
        try:
            # Generate 5-15 messages per batch
            batch_size = random.randint(5, 15)

            with messages_lock:
                for _ in range(batch_size):
                    msg = generate_price_update()
                    messages.append(msg)

            # Generate messages every 2-5 seconds
            time.sleep(random.uniform(2, 5))

        except Exception as e:
            print(f"[GENERATOR] Error: {e}")
            time.sleep(5)

# Start message generator in background
generator_thread = threading.Thread(target=message_generator, daemon=True)
generator_thread.start()

@app.route('/pipe/poll', methods=['GET'])
def poll_messages():
    """
    Endpoint for child brokers to poll messages
    Query params:
      - offset: Starting offset (default: 0)
      - limit: Max number of messages (default: 100)
      - topic: Topic name (optional, ignored in this test server)
    """
    offset = int(request.args.get('offset', 0))
    limit = int(request.args.get('limit', 100))
    topic = request.args.get('topic', 'price-topic')

    print(f"[CLOUD] Poll request: offset={offset}, limit={limit}, topic={topic}")

    with messages_lock:
        total_messages = len(messages)

        # Get messages from offset
        if offset >= total_messages:
            print(f"[CLOUD] No new messages (offset {offset} >= {total_messages} total messages)")
            return '', 204  # No Content

        # Slice messages
        end_offset = min(offset + limit, total_messages)
        result = messages[offset:end_offset]

    print(f"[CLOUD] Serving {len(result)} messages (offset {offset} to {end_offset-1})")
    for msg in result[:5]:  # Only print first 5 to avoid spam
        print(f"  - {msg['msgKey']}")
    if len(result) > 5:
        print(f"  ... and {len(result) - 5} more")

    return jsonify(result), 200

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    with messages_lock:
        total = len(messages)

    return jsonify({
        "status": "healthy",
        "role": "CLOUD",
        "totalMessages": total
    }), 200

@app.route('/status', methods=['GET'])
def status():
    """Status endpoint showing available messages"""
    with messages_lock:
        total = len(messages)
        # Only show last 100 messages to avoid huge response
        recent_messages = [{"offset": i, "key": msg["msgKey"]} for i, msg in enumerate(messages[-100:])]

    return jsonify({
        "role": "CLOUD",
        "totalMessages": total,
        "recentMessages": recent_messages,
        "showing": "last 100 messages"
    }), 200

@app.route('/registry/topology', methods=['GET'])
def get_topology():
    """
    Cloud Registry endpoint - returns broker topology information
    Query params:
      - nodeId: The requesting broker's ID (e.g., "broker-001", "pos-123")

    Returns TopologyResponse with:
      - nodeId: The broker's ID
      - role: ROOT, L2, L3, or POS
      - requestToFollow: List of parent URLs to connect to
      - cloudDataUrl: Cloud API URL (for ROOT only)
      - topics: List of topics to subscribe to
    """
    node_id = request.args.get('nodeId', 'unknown')

    print(f"[REGISTRY] Topology request from: {node_id}")

    # Simple topology mapping based on nodeId prefix
    # In production, this would query a database
    if node_id.startswith('root') or node_id == 'broker-root':
        # ROOT broker - no parent, polls Cloud directly
        response = {
            "nodeId": node_id,
            "role": "ROOT",
            "requestToFollow": [],  # No parent
            "cloudDataUrl": "http://localhost:8080",  # This server
            "cloudDataUrlFallback": "http://localhost:8080",
            "topologyVersion": "1.0",
            "topics": ["price-topic", "trade-topic"]
        }
    elif node_id.startswith('l2') or node_id.startswith('broker-l2'):
        # L2 broker - parent is ROOT
        response = {
            "nodeId": node_id,
            "role": "L2",
            "requestToFollow": ["http://localhost:9080"],  # ROOT broker HTTP
            "cloudDataUrl": None,
            "cloudDataUrlFallback": None,
            "topologyVersion": "1.0",
            "topics": ["price-topic", "trade-topic"]
        }
    elif node_id.startswith('l3') or node_id.startswith('broker-l3'):
        # L3 broker - parent is L2
        response = {
            "nodeId": node_id,
            "role": "L3",
            "requestToFollow": ["http://localhost:9081"],  # L2 broker HTTP
            "cloudDataUrl": None,
            "cloudDataUrlFallback": None,
            "topologyVersion": "1.0",
            "topics": ["price-topic", "trade-topic"]
        }
    elif node_id.startswith('pos') or node_id.startswith('broker-pos'):
        # POS broker - parent is L3
        response = {
            "nodeId": node_id,
            "role": "POS",
            "requestToFollow": ["http://localhost:9082"],  # L3 broker HTTP
            "cloudDataUrl": None,
            "cloudDataUrlFallback": None,
            "topologyVersion": "1.0",
            "topics": ["price-topic", "trade-topic"]
        }
    else:
        # Default: treat as LOCAL broker polling Cloud directly
        response = {
            "nodeId": node_id,
            "role": "LOCAL",
            "requestToFollow": ["http://cloud-server:8080"],  # Cloud
            "cloudDataUrl": "http://cloud-server:8080",
            "cloudDataUrlFallback": None,
            "topologyVersion": "1.0",
            "topics": ["price-topic"]
        }

    print(f"[REGISTRY] Assigned role: {response['role']}, parent: {response['requestToFollow']}")
    return jsonify(response), 200

if __name__ == '__main__':
    print("=" * 70)
    print("  CLOUD Test Server")
    print("=" * 70)
    print("  Port:          8080")
    print("  Endpoint:      http://localhost:8080/pipe/poll")
    print("  Total Messages:", len(messages))
    print("=" * 70)
    print("\nTest URLs:")
    print("  - Health:   http://localhost:8080/health")
    print("  - Status:   http://localhost:8080/status")
    print("  - Registry: http://localhost:8080/registry/topology?nodeId=local-001")
    print("  - Poll:     http://localhost:8080/pipe/poll?offset=0&limit=10")
    print("\n" + "=" * 70)
    print("Starting server...")
    print("=" * 70 + "\n")

    app.run(host='0.0.0.0', port=8080, debug=True)
