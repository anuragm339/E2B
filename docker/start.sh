#!/bin/bash
set -e

# MagicPipe Provider Startup Script
echo "========================================"
echo "Starting MagicPipe Provider"
echo "========================================"

# Environment variables with defaults
export NODE_ID="${NODE_ID:-broker-001}"
export BROKER_PORT="${BROKER_PORT:-9092}"
export HTTP_PORT="${HTTP_PORT:-8081}"
export DATA_DIR="${DATA_DIR:-/var/data/magicpipe}"
export REGISTRY_URL="${REGISTRY_URL:-http://localhost:8080}"
export AUTH_BEARER_TOKEN="${AUTH_BEARER_TOKEN:-}"

# Java options
export JAVA_OPTS="${JAVA_OPTS:--Xms512m -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200}"

# Enable remote debugging if DEBUG_PORT is set
if [ -n "$DEBUG_PORT" ]; then
    JAVA_OPTS="$JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:${DEBUG_PORT}"
    echo "Remote debugging enabled on port ${DEBUG_PORT}"
fi

# Log configuration
echo "Configuration:"
echo "  NODE_ID: ${NODE_ID}"
echo "  BROKER_PORT: ${BROKER_PORT}"
echo "  HTTP_PORT: ${HTTP_PORT}"
echo "  DATA_DIR: ${DATA_DIR}"
echo "  REGISTRY_URL: ${REGISTRY_URL}"
echo "  JAVA_OPTS: ${JAVA_OPTS}"
echo "========================================"

# Ensure data directory exists
mkdir -p "${DATA_DIR}"

# Find the provider jar
JAR_FILE=$(ls /opt/magicpipe_provider/provider-*-all.jar 2>/dev/null | head -1)

if [ -z "$JAR_FILE" ]; then
    echo "ERROR: Provider JAR not found in /opt/magicpipe_provider/"
    exit 1
fi

echo "Starting JAR: ${JAR_FILE}"

# Start the application
exec java ${JAVA_OPTS} \
    -jar "${JAR_FILE}" \
    --micronaut.config.files=/etc/magicpipe_provider/application.yml
