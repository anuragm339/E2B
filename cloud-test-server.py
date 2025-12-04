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

# All topics served by cloud server (24 topics total)
ALL_TOPICS = [
    "prices-v1", "reference-data-v5", "non-promotable-products", "prices-v4",
    "minimum-price", "deposit", "product-base-document", "search-product",
    "location", "location-clusters", "selling-restrictions", "colleague-facts-jobs",
    "colleague-facts-legacy", "loss-prevention-configuration",
    "loss-prevention-store-configuration", "loss-prevention-product",
    "loss-prevention-rule-config", "stored-value-services-banned-promotion",
    "stored-value-services-active-promotion", "colleague-card-pin",
    "colleague-card-pin-v2", "dcxp-content", "restriction-rules", "dcxp-ugc"
]

# Data generators for different topic types
DATA_TEMPLATES = {
    "price": lambda: {"price": round(random.uniform(10, 1000), 2), "currency": "GBP", "timestamp": datetime.utcnow().isoformat() + "Z"},
    "product": lambda: {"productId": random.randint(10000, 99999), "name": f"Product-{random.randint(1, 999)}", "category": random.choice(["Food", "Electronics", "Clothing"])},
    "location": lambda: {"storeId": random.randint(1000, 9999), "latitude": round(random.uniform(50, 55), 6), "longitude": round(random.uniform(-5, 2), 6)},
    "restriction": lambda: {"restrictionId": random.randint(1, 100), "type": random.choice(["AGE", "QUANTITY", "TIME"]), "value": random.randint(1, 21)},
    "colleague": lambda: {"colleagueId": random.randint(10000, 99999), "role": random.choice(["CASHIER", "MANAGER", "STOCK"]), "storeId": random.randint(1000, 9999)},
    "prevention": lambda: {"ruleId": random.randint(1, 500), "severity": random.choice(["LOW", "MEDIUM", "HIGH"]), "action": random.choice(["ALERT", "BLOCK"])},
    "content": lambda: {"contentId": random.randint(1, 10000), "type": random.choice(["BANNER", "PROMOTION", "NEWS"]), "status": "ACTIVE"}
}

# Lock for thread-safe message append
messages_lock = threading.Lock()

def get_template_for_topic(topic):
    """Map topic to data template"""
    if any(x in topic for x in ["price", "minimum", "deposit", "reference"]):
        return "price"
    elif any(x in topic for x in ["product", "search", "promotable"]):
        return "product"
    elif any(x in topic for x in ["location"]):
        return "location"
    elif any(x in topic for x in ["restriction", "selling"]):
        return "restriction"
    elif any(x in topic for x in ["colleague", "identity"]):
        return "colleague"
    elif any(x in topic for x in ["prevention", "loss"]):
        return "prevention"
    else:
        return "content"

def generate_message():
    """Generate a message for a random topic"""
    topic = random.choice(ALL_TOPICS)
    template_type = get_template_for_topic(topic)
    data = DATA_TEMPLATES[template_type]()

    # Add topic-specific fields
    data["topic"] = topic
    data["generatedAt"] = datetime.utcnow().isoformat() + "Z"

    # Generate message with size up to ~1KB (expandable to 1MB if needed)
    # Add padding to simulate realistic message sizes
    data["metadata"] = {
        "version": "1.0",
        "source": "cloud-server",
        "messageId": len(messages),
        "padding": "x" * random.randint(100, 500)  # Random padding
    }

    message = {
        "msgKey": f"{topic}_{len(messages)}",
        "eventType": "MESSAGE",
        "topic": topic,
        "data": json.dumps(data),
        "createdAt": datetime.utcnow().isoformat() + "Z"
    }

    return message

def message_generator():
    """Background thread to continuously generate messages"""
    print("[GENERATOR] Starting message generator thread for all 24 topics...")

    while True:
        try:
            # Generate 10-30 messages per batch (to cover all topics)
            batch_size = random.randint(10, 30)

            with messages_lock:
                for _ in range(batch_size):
                    msg = generate_message()
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
