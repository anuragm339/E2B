#!/usr/bin/env python3
"""
Cloud Test Server with Actual Data Injection Support
Simulates a Cloud API that serves messages to downstream brokers via /pipe/poll

USAGE:
1. For Testing: Uses random data generation (default)
2. For Production: Replace inject_actual_data() with your actual data source
"""

from flask import Flask, jsonify, request
from datetime import datetime
import json
import time
import random
import threading
import os

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

# Data generators for different topic types (TEST MODE)
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

# Mode: TEST or PRODUCTION
DATA_MODE = os.getenv('DATA_MODE', 'TEST')  # Set DATA_MODE=PRODUCTION for actual data

# ============================================================================
# ACTUAL DATA INJECTION FUNCTIONS - REPLACE THESE FOR PRODUCTION
# ============================================================================

def inject_actual_data():
    """
    **REPLACE THIS FUNCTION** with your actual data source

    This function should:
    1. Connect to your data source (database, API, file, Kafka, etc.)
    2. Read actual messages
    3. Format them according to the schema below
    4. Add them to the messages list

    Message Schema:
    {
        "msgKey": "unique_message_id",      # Unique message identifier
        "eventType": "MESSAGE" or "DELETE",  # Message type
        "topic": "topic-name",               # Topic name (must be in ALL_TOPICS)
        "data": "json_string_data",          # Your actual data as JSON string
        "createdAt": "2025-12-04T10:00:00Z"  # ISO 8601 timestamp
    }

    Example implementations:

    # Example 1: Reading from a database
    def inject_actual_data():
        import psycopg2
        conn = psycopg2.connect("dbname=mydb user=user password=pass")
        cursor = conn.execute("SELECT id, topic, data, created_at FROM messages WHERE processed=false")

        for row in cursor.fetchall():
            message = {
                "msgKey": f"db_{row[0]}",
                "eventType": "MESSAGE",
                "topic": row[1],
                "data": json.dumps(row[2]),
                "createdAt": row[3].isoformat() + "Z"
            }
            with messages_lock:
                messages.append(message)

            # Mark as processed
            conn.execute("UPDATE messages SET processed=true WHERE id=%s", (row[0],))

        conn.commit()
        conn.close()

    # Example 2: Reading from a Kafka topic
    def inject_actual_data():
        from kafka import KafkaConsumer
        consumer = KafkaConsumer('source-topic', bootstrap_servers=['localhost:9092'])

        for kafka_msg in consumer:
            message = {
                "msgKey": f"kafka_{kafka_msg.offset}",
                "eventType": "MESSAGE",
                "topic": determine_topic(kafka_msg.value),  # Map to your topics
                "data": kafka_msg.value.decode('utf-8'),
                "createdAt": datetime.utcnow().isoformat() + "Z"
            }
            with messages_lock:
                messages.append(message)

    # Example 3: Reading from a CSV file
    def inject_actual_data():
        import csv
        with open('/data/messages.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                message = {
                    "msgKey": row['id'],
                    "eventType": "MESSAGE",
                    "topic": row['topic'],
                    "data": json.dumps(row),
                    "createdAt": row.get('timestamp', datetime.utcnow().isoformat() + "Z")
                }
                with messages_lock:
                    messages.append(message)

    # Example 4: Reading from an external API
    def inject_actual_data():
        import requests
        response = requests.get('https://api.example.com/messages')
        data = response.json()

        for item in data['messages']:
            message = {
                "msgKey": item['id'],
                "eventType": "MESSAGE",
                "topic": item['category'],  # Map category to topic
                "data": json.dumps(item),
                "createdAt": item['timestamp']
            }
            with messages_lock:
                messages.append(message)
    """
    # TODO: Replace this with your actual data source
    # For now, this is a placeholder that does nothing
    print("[ACTUAL-DATA] inject_actual_data() called - Replace this function with your data source!")
    print("[ACTUAL-DATA] See comments above for implementation examples")
    time.sleep(5)  # Placeholder delay

# ============================================================================
# END OF ACTUAL DATA INJECTION SECTION
# ============================================================================

def get_template_for_topic(topic):
    """Map topic to data template (TEST MODE only)"""
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

def generate_test_message():
    """Generate a test message for a random topic (TEST MODE)"""
    topic = random.choice(ALL_TOPICS)
    template_type = get_template_for_topic(topic)
    data = DATA_TEMPLATES[template_type]()

    # Add topic-specific fields
    data["topic"] = topic
    data["generatedAt"] = datetime.utcnow().isoformat() + "Z"

    # Generate message with size up to ~1KB (expandable to 1MB if needed)
    data["metadata"] = {
        "version": "1.0",
        "source": "cloud-server",
        "messageId": len(messages),
        "padding": "x" * random.randint(100, 500)
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
    """Background thread to continuously generate or inject messages"""
    print(f"[GENERATOR] Starting message generator in {DATA_MODE} mode...")

    if DATA_MODE == 'PRODUCTION':
        print("[GENERATOR] PRODUCTION MODE - Using inject_actual_data()")
        print("[GENERATOR] **REMINDER**: Replace inject_actual_data() with your actual data source!")
    else:
        print("[GENERATOR] TEST MODE - Using random data generation")

    while True:
        try:
            if DATA_MODE == 'PRODUCTION':
                # PRODUCTION MODE: Call inject_actual_data()
                inject_actual_data()
            else:
                # TEST MODE: Generate random messages
                batch_size = random.randint(10, 30)

                with messages_lock:
                    for _ in range(batch_size):
                        msg = generate_test_message()
                        messages.append(msg)

                # Generate messages every 2-5 seconds
                time.sleep(random.uniform(2, 5))

        except Exception as e:
            print(f"[GENERATOR] Error: {e}")
            import traceback
            traceback.print_exc()
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
        "mode": DATA_MODE,
        "totalMessages": total
    }), 200

@app.route('/status', methods=['GET'])
def status():
    """Status endpoint showing available messages"""
    with messages_lock:
        total = len(messages)
        recent_messages = [{"offset": i, "key": msg["msgKey"]} for i, msg in enumerate(messages[-100:])]

    return jsonify({
        "role": "CLOUD",
        "mode": DATA_MODE,
        "totalMessages": total,
        "recentMessages": recent_messages,
        "showing": "last 100 messages"
    }), 200

@app.route('/registry/topology', methods=['GET'])
def get_topology():
    """
    Cloud Registry endpoint - returns broker topology information
    """
    node_id = request.args.get('nodeId', 'unknown')
    print(f"[REGISTRY] Topology request from: {node_id}")

    # Default: treat as LOCAL broker polling Cloud directly
    response = {
        "nodeId": node_id,
        "role": "LOCAL",
        "requestToFollow": ["http://cloud-server:8080"],
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
    print(f"  Mode:          {DATA_MODE}")
    print("  Port:          8080")
    print("  Endpoint:      http://localhost:8080/pipe/poll")
    print("  Total Messages:", len(messages))
    print("=" * 70)
    if DATA_MODE == 'PRODUCTION':
        print("\n⚠️  PRODUCTION MODE ACTIVE")
        print("  Replace inject_actual_data() with your data source!")
        print("=" * 70)
    print("\nTest URLs:")
    print("  - Health:   http://localhost:8080/health")
    print("  - Status:   http://localhost:8080/status")
    print("  - Registry: http://localhost:8080/registry/topology?nodeId=local-001")
    print("  - Poll:     http://localhost:8080/pipe/poll?offset=0&limit=10")
    print("\n" + "=" * 70)
    print("Starting server...")
    print("=" * 70 + "\n")

    app.run(host='0.0.0.0', port=8080, debug=False)
