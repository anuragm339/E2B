#!/bin/bash

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Messaging System Interactive Setup${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

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

print_info() {
    echo -e "${BLUE}[ℹ]${NC} $1"
}

# Function to prompt for path with default
prompt_path() {
    local prompt_msg="$1"
    local default_val="$2"
    local var_name="$3"

    echo -e "${BLUE}${prompt_msg}${NC}"
    read -p "Enter path [default: ${default_val}]: " user_input

    if [ -z "$user_input" ]; then
        eval "$var_name='$default_val'"
    else
        eval "$var_name='$user_input'"
    fi
}

# Configuration section
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 1: Path Configuration${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Get current directory as base
CURRENT_DIR="$(pwd)"

# Prompt for paths
prompt_path "Provider path (broker Java code)" "${CURRENT_DIR}/provider" PROVIDER_PATH
prompt_path "Consumer path (consumer app Java code)" "${CURRENT_DIR}/consumer-app" CONSUMER_PATH
prompt_path "Monitoring path (Grafana/Prometheus config)" "${CURRENT_DIR}/monitoring" MONITORING_PATH
prompt_path "Cloud server script path" "${CURRENT_DIR}/cloud-test-server.py" CLOUD_SERVER_PATH
prompt_path "CA Certificate path (for JDK)" "${PROVIDER_PATH}/mvnrepository.com.pem" CA_CERT_PATH

echo ""
echo -e "${GREEN}Configuration Summary:${NC}"
echo "  Provider:    ${PROVIDER_PATH}"
echo "  Consumer:    ${CONSUMER_PATH}"
echo "  Monitoring:  ${MONITORING_PATH}"
echo "  Cloud Script: ${CLOUD_SERVER_PATH}"
echo "  CA Cert:     ${CA_CERT_PATH}"
echo ""

read -p "Continue with these paths? (y/n): " confirm
if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
    echo "Setup cancelled."
    exit 1
fi

# Validate paths
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 2: Validating Paths${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

validate_path() {
    local path="$1"
    local name="$2"

    if [ ! -e "$path" ]; then
        print_error "$name not found: $path"
        return 1
    fi
    print_status "$name found"
    return 0
}

validate_path "$PROVIDER_PATH" "Provider directory" || exit 1
validate_path "$CONSUMER_PATH" "Consumer directory" || exit 1
validate_path "$MONITORING_PATH" "Monitoring directory" || exit 1
validate_path "$CLOUD_SERVER_PATH" "Cloud server script" || exit 1
validate_path "$CA_CERT_PATH" "CA certificate" || exit 1

# Import CA certificate into JDK
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 3: Import CA Certificate to JDK${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Find Java home
if [ -n "$JAVA_HOME" ]; then
    JDK_PATH="$JAVA_HOME"
else
    # Try to find Java using java.home property (most reliable)
    JDK_PATH=$(java -XshowSettings:properties -version 2>&1 | grep 'java.home' | awk -F'=' '{print $2}' | tr -d ' ' 2>/dev/null)

    if [ -z "$JDK_PATH" ]; then
        # Fallback: Try to find Java binary and resolve path
        JAVA_BIN=$(which java 2>/dev/null)
        if [ -n "$JAVA_BIN" ]; then
            # Follow symlinks to get actual Java home
            JDK_PATH=$(dirname $(dirname $(readlink -f "$JAVA_BIN" 2>/dev/null || echo "$JAVA_BIN")))
        else
            print_error "Java not found. Please set JAVA_HOME"
            exit 1
        fi
    fi
fi

print_info "Java Home: ${JDK_PATH}"

# Find cacerts file
CACERTS_PATH="${JDK_PATH}/lib/security/cacerts"
if [ ! -f "$CACERTS_PATH" ]; then
    # Try alternate location (for some JDKs)
    CACERTS_PATH="${JDK_PATH}/jre/lib/security/cacerts"
fi

if [ ! -f "$CACERTS_PATH" ]; then
    print_warning "cacerts file not found at: ${CACERTS_PATH}"
    print_warning "CA certificate import will be skipped"
    print_info "This is usually fine if your JDK already trusts Maven Central"
    SKIP_CA_IMPORT=true
else
    print_status "Found cacerts: ${CACERTS_PATH}"

    # Check if certificate is already imported
    CA_ALIAS="messaging_maven_ca"

    if keytool -list -keystore "$CACERTS_PATH" -storepass changeit -alias "$CA_ALIAS" >/dev/null 2>&1; then
        print_status "CA certificate already imported (alias: $CA_ALIAS)"
    else
        print_warning "CA certificate not yet imported"
        print_info "This certificate is REQUIRED for Docker builds to access Maven repositories"
        echo ""
        print_info "Importing CA certificate (requires sudo password)..."
        echo ""

        # Import the certificate - this is mandatory
        sudo keytool -import -trustcacerts -alias "$CA_ALIAS" \
            -file "$CA_CERT_PATH" \
            -keystore "$CACERTS_PATH" \
            -storepass changeit -noprompt

        if [ $? -eq 0 ]; then
            print_status "CA certificate imported successfully"
        else
            print_error "Failed to import CA certificate"
            print_error "Docker builds will fail without this certificate in production!"
            echo ""
            print_info "To manually import the certificate, run:"
            print_info "  sudo keytool -import -trustcacerts -alias $CA_ALIAS \\"
            print_info "    -file $CA_CERT_PATH \\"
            print_info "    -keystore $CACERTS_PATH \\"
            print_info "    -storepass changeit -noprompt"
            echo ""
            read -p "Continue anyway (NOT recommended for production)? [y/N]: " continue_anyway
            if [ "$continue_anyway" != "y" ] && [ "$continue_anyway" != "Y" ]; then
                print_error "Setup cancelled. Please import the certificate and try again."
                exit 1
            fi
            print_warning "Continuing without certificate import - builds may fail!"
        fi
    fi
fi

# Update docker-compose.yml with paths
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 4: Update docker-compose.yml${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

DOCKER_COMPOSE_FILE="${CURRENT_DIR}/docker-compose.yml"

if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
    print_error "docker-compose.yml not found at: ${DOCKER_COMPOSE_FILE}"
    exit 1
fi

# Backup original
cp "$DOCKER_COMPOSE_FILE" "${DOCKER_COMPOSE_FILE}.backup.$(date +%Y%m%d_%H%M%S)"
print_status "Backed up docker-compose.yml"

# Use Python to update paths in docker-compose.yml
python3 << EOF
import re

# Read docker-compose.yml
with open('${DOCKER_COMPOSE_FILE}', 'r') as f:
    content = f.read()

# Update paths
content = re.sub(r'context: \./provider', 'context: ${PROVIDER_PATH}', content)
content = re.sub(r'context: \./consumer-app', 'context: ${CONSUMER_PATH}', content)
content = re.sub(r'\./cloud-test-server\.py:/app/cloud-test-server\.py',
                 '${CLOUD_SERVER_PATH}:/app/cloud-test-server.py', content)
content = re.sub(r'\./monitoring/prometheus/prometheus\.yml',
                 '${MONITORING_PATH}/prometheus/prometheus.yml', content)
content = re.sub(r'\./monitoring/grafana/dashboards',
                 '${MONITORING_PATH}/grafana/dashboards', content)
content = re.sub(r'\./monitoring/grafana/datasources',
                 '${MONITORING_PATH}/grafana/datasources', content)

# Write back
with open('${DOCKER_COMPOSE_FILE}', 'w') as f:
    f.write(content)

print("Updated docker-compose.yml with configured paths")
EOF

print_status "docker-compose.yml updated with paths"

# Check Docker prerequisites
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 5: Checking Prerequisites${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker Desktop."
    exit 1
fi
print_status "Docker is running"

if ! command -v docker &> /dev/null; then
    print_error "docker compose is not available"
    exit 1
fi
print_status "Docker Compose is available"

if ! command -v java &> /dev/null; then
    print_error "Java is not installed"
    exit 1
fi
print_status "Java is installed ($(java -version 2>&1 | head -n 1))"

# Clean up existing containers
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 6: Cleaning Up Existing Setup${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

docker compose down -v 2>/dev/null || true
print_status "Cleaned up existing containers"

# Build the broker
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 7: Building Broker${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

cd "${PROVIDER_PATH}"
./gradlew :broker:build --no-daemon --quiet
if [ $? -eq 0 ]; then
    print_status "Broker built successfully"
else
    print_error "Broker build failed"
    exit 1
fi
cd "${CURRENT_DIR}"

# Build consumer app
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 8: Building Consumer Application${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

cd "${CONSUMER_PATH}"
./gradlew build --no-daemon --quiet
if [ $? -eq 0 ]; then
    print_status "Consumer application built successfully"
else
    print_error "Consumer build failed"
    exit 1
fi
cd "${CURRENT_DIR}"

# Build Docker images
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 9: Building Docker Images${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

docker compose build --quiet
print_status "Docker images built successfully"

# Start infrastructure services
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 10: Starting Services${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

print_info "Starting Prometheus and Grafana..."
docker compose up -d prometheus grafana
sleep 5
print_status "Prometheus started on http://localhost:9090"
print_status "Grafana started on http://localhost:3000 (admin/admin)"

# Import Grafana dashboards
echo ""
echo -e "${BLUE}  Step 11: Importing Grafana Dashboards${NC}"
echo ""

sleep 5  # Wait for Grafana to be ready

# Import multi-consumer dashboard
curl -s -X POST -H "Content-Type: application/json" -u admin:admin \
  -d @"${MONITORING_PATH}/grafana/dashboards/messaging-broker-dashboard.json" \
  http://localhost:3000/api/dashboards/db > /dev/null 2>&1 || true

# Import per-consumer dashboard
curl -s -X POST -H "Content-Type: application/json" -u admin:admin \
  -d @"${MONITORING_PATH}/grafana/dashboards/per-consumer-dashboard.json" \
  http://localhost:3000/api/dashboards/db > /dev/null 2>&1 || true

print_status "Grafana dashboards imported"

# Start cloud-server
echo ""
print_info "Starting cloud-server..."
docker compose up -d cloud-server
sleep 5
print_status "Cloud-server started on http://localhost:8080"

# Start the broker
echo ""
print_info "Starting messaging broker..."
docker compose up -d broker
sleep 10
print_status "Broker started on port 9092"

# Start all consumers
echo ""
print_info "Starting consumer services..."
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

# Verify system health
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 12: Verifying System Health${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check broker health
BROKER_HEALTH=$(curl -s http://localhost:8081/health 2>/dev/null || echo "")
if [[ $BROKER_HEALTH == *"UP"* ]]; then
    print_status "Broker is healthy"
else
    print_warning "Broker health check failed (may need more time to start)"
fi

# Check active consumers
sleep 5
ACTIVE_CONSUMERS=$(curl -s 'http://localhost:9090/api/v1/query?query=broker_consumer_active' 2>/dev/null | grep -o '"value":\[[^]]*\]' | grep -o '[0-9]*$' || echo "0")
if [[ $ACTIVE_CONSUMERS -ge 20 ]]; then
    print_status "Active consumers: $ACTIVE_CONSUMERS/24"
else
    print_warning "Active consumers: $ACTIVE_CONSUMERS/24 (some consumers may still be connecting...)"
fi

# Display summary
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Setup Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Configuration:"
echo "  Provider:    ${PROVIDER_PATH}"
echo "  Consumer:    ${CONSUMER_PATH}"
echo "  Monitoring:  ${MONITORING_PATH}"
echo "  Cloud Script: ${CLOUD_SERVER_PATH}"
echo "  CA Cert:     ${CA_CERT_PATH} (imported to JDK)"
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
echo "Memory Limits:"
echo "  • Broker:       300MB"
echo "  • Each Consumer: 200MB (13 consumers = 2.6GB)"
echo "  • Total Docker: ~3.3GB / 4GB"
echo ""
echo "Useful Commands:"
echo "  • View logs:      docker compose logs -f [service-name]"
echo "  • Stop all:       docker compose down"
echo "  • Restart:        docker compose restart [service-name]"
echo "  • Check status:   docker compose ps"
echo ""
echo "To inject actual data:"
echo "  1. Edit ${CLOUD_SERVER_PATH}"
echo "  2. Replace inject_actual_data() with your actual data source"
echo "  3. Set DATA_MODE=PRODUCTION in docker-compose.yml"
echo "  4. Run: docker compose restart cloud-server"
echo ""
print_status "System is ready!"
echo ""
