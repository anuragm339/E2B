#!/bin/bash

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Messaging System Setup Script${NC}"
echo -e "${GREEN}========================================${NC}"

# Function to print status
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Check if Docker is running
echo ""
echo "Checking prerequisites..."
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker Desktop."
    exit 1
fi
print_status "Docker is running"

# Check if docker-compose is available
if ! command -v docker &> /dev/null; then
    print_error "docker compose is not available"
    exit 1
fi
print_status "Docker Compose is available"

# Check if Java is installed
if ! command -v java &> /dev/null; then
    print_error "Java is not installed"
    exit 1
fi
print_status "Java is installed ($(java -version 2>&1 | head -n 1))"

# Step 1: Clean up existing containers and volumes
echo ""
echo "Step 1: Cleaning up existing containers..."
docker compose down -v 2>/dev/null || true
print_status "Cleaned up existing containers"

# Step 2: Build the broker
echo ""
echo "Step 2: Building the broker..."
cd provider
./gradlew :broker:build --no-daemon --quiet
cd ..
print_status "Broker built successfully"

# Step 3: Build consumer app
echo ""
echo "Step 3: Building consumer application..."
cd consumer-app
./gradlew build --no-daemon --quiet
cd ..
print_status "Consumer application built successfully"

# Step 4: Build Docker images
echo ""
echo "Step 4: Building Docker images..."
docker compose build --quiet
print_status "Docker images built successfully"

# Step 5: Start the infrastructure services
echo ""
echo "Step 5: Starting infrastructure services (Prometheus, Grafana)..."
docker compose up -d prometheus grafana
sleep 5
print_status "Prometheus started on http://localhost:9090"
print_status "Grafana started on http://localhost:3000 (admin/admin)"

# Step 6: Import Grafana dashboards
echo ""
echo "Step 6: Importing Grafana dashboards..."
sleep 5  # Wait for Grafana to be ready

# Import multi-consumer dashboard
curl -s -X POST -H "Content-Type: application/json" -u admin:admin \
  -d @monitoring/grafana/dashboards/messaging-broker-dashboard.json \
  http://localhost:3000/api/dashboards/db > /dev/null 2>&1 || true

# Import per-consumer dashboard
curl -s -X POST -H "Content-Type: application/json" -u admin:admin \
  -d @monitoring/grafana/dashboards/per-consumer-dashboard.json \
  http://localhost:3000/api/dashboards/db > /dev/null 2>&1 || true

print_status "Grafana dashboards imported"

# Step 7: Start cloud-server
echo ""
echo "Step 7: Starting cloud-server..."
docker compose up -d cloud-server
sleep 5
print_status "Cloud-server started on http://localhost:8080"

# Step 8: Start the broker
echo ""
echo "Step 8: Starting messaging broker..."
docker compose up -d broker
sleep 10
print_status "Broker started on port 9092"

# Step 9: Start all consumers
echo ""
echo "Step 9: Starting consumer services..."
docker compose up -d \
  consumer-price-quote \
  consumer-product-svc-lite \
  consumer-search-enterprise \
  consumer-tesco-location-service \
  consumer-customer-order-on-till \
  consumer-colleague-facts \
  consumer-loss-prevention-api \
  consumer-stored-value-services \
  consumer-colleague-identity \
  consumer-distributed-identity \
  consumer-dcxp-content \
  consumer-restriction-service-on-tills \
  consumer-dcxp-ugc

sleep 15
print_status "All 13 consumer services started"

# Step 10: Verify system health
echo ""
echo "Step 10: Verifying system health..."

# Check broker health
BROKER_HEALTH=$(curl -s http://localhost:8081/health 2>/dev/null || echo "")
if [[ $BROKER_HEALTH == *"UP"* ]]; then
    print_status "Broker is healthy"
else
    print_warning "Broker health check failed"
fi

# Check active consumers
ACTIVE_CONSUMERS=$(curl -s 'http://localhost:9090/api/v1/query?query=broker_consumer_active' 2>/dev/null | grep -o '"value":\[[^]]*\]' | grep -o '[0-9]*$' || echo "0")
if [[ $ACTIVE_CONSUMERS -ge 20 ]]; then
    print_status "Active consumers: $ACTIVE_CONSUMERS/24"
else
    print_warning "Active consumers: $ACTIVE_CONSUMERS/24 (waiting for connections...)"
fi

# Step 11: Display summary
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Setup Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Services:"
echo "  • Broker:       http://localhost:8081/health"
echo "  • Prometheus:   http://localhost:9090"
echo "  • Grafana:      http://localhost:3000 (admin/admin)"
echo "  • Cloud Server: http://localhost:8080/health"
echo ""
echo "Grafana Dashboards:"
echo "  • Multi-Consumer: http://localhost:3000/d/messaging-broker-multi-consumer"
echo "  • Per-Consumer:   http://localhost:3000/d/per-consumer-metrics"
echo ""
echo "Useful Commands:"
echo "  • View logs:      docker compose logs -f [service-name]"
echo "  • Stop all:       docker compose down"
echo "  • Restart:        docker compose restart [service-name]"
echo "  • Check status:   docker compose ps"
echo ""
echo "To inject actual data:"
echo "  1. Edit cloud-server/cloud-server.py"
echo "  2. Replace generate_random_message() with your actual data"
echo "  3. Run: docker compose restart cloud-server"
echo ""
print_status "System is ready!"
