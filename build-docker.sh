#!/bin/bash
set -e

echo "========================================"
echo "Building MagicPipe Provider Docker Image"
echo "========================================"

# Build the Micronaut runner JAR
echo "Step 1: Building Micronaut runner JAR with Gradle..."
./gradlew :broker:build -x test

# Check if JAR was created
JAR_FILE="broker/build/libs/broker-runner.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: Runner JAR not found at ${JAR_FILE}"
    exit 1
fi

echo "✅ JAR built: ${JAR_FILE}"

# Build Docker image
echo "Step 2: Building Docker image..."
IMAGE_NAME="${1:-magicpipe-provider:latest}"
docker build -t "${IMAGE_NAME}" .

echo "========================================"
echo "✅ Docker image built successfully!"
echo "Image: ${IMAGE_NAME}"
echo "========================================"
echo ""
echo "To run the container:"
echo "  docker run -d -p 8081:8081 -p 9092:9092 \\"
echo "    -e NODE_ID=broker-001 \\"
echo "    -e REGISTRY_URL=http://cloud-server:8080 \\"
echo "    ${IMAGE_NAME}"
