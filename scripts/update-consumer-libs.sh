#!/bin/bash

##############################################################################
# Update Consumer Libraries Script
#
# This script rebuilds the common and network modules from the provider
# project and copies the JAR files to the consumer-app libs directory.
#
# Usage: ./update-consumer-libs.sh [--skip-build]
#   --skip-build: Skip rebuilding, just copy existing JARs
##############################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROVIDER_DIR="$SCRIPT_DIR/provider"
CONSUMER_DIR="$SCRIPT_DIR/consumer-app"
CONSUMER_LIBS_DIR="$CONSUMER_DIR/libs"

# JAR paths
COMMON_JAR="$PROVIDER_DIR/common/build/libs/common.jar"
NETWORK_JAR="$PROVIDER_DIR/network/build/libs/network.jar"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Update Consumer Libraries Script${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if --skip-build flag is provided
SKIP_BUILD=false
if [ "$1" == "--skip-build" ]; then
    SKIP_BUILD=true
    echo -e "${YELLOW}⚠ Skipping build, will copy existing JARs${NC}"
    echo ""
fi

# Check if provider directory exists
if [ ! -d "$PROVIDER_DIR" ]; then
    echo -e "${RED}✗ Error: Provider directory not found: $PROVIDER_DIR${NC}"
    exit 1
fi

# Check if consumer directory exists
if [ ! -d "$CONSUMER_DIR" ]; then
    echo -e "${RED}✗ Error: Consumer directory not found: $CONSUMER_DIR${NC}"
    exit 1
fi

# Create libs directory if it doesn't exist
if [ ! -d "$CONSUMER_LIBS_DIR" ]; then
    echo -e "${YELLOW}Creating libs directory: $CONSUMER_LIBS_DIR${NC}"
    mkdir -p "$CONSUMER_LIBS_DIR"
fi

# Build JARs if not skipping
if [ "$SKIP_BUILD" = false ]; then
    echo -e "${BLUE}Step 1: Building common module...${NC}"
    cd "$PROVIDER_DIR"
    ./gradlew :common:build -x test --quiet

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Common module built successfully${NC}"
    else
        echo -e "${RED}✗ Failed to build common module${NC}"
        exit 1
    fi
    echo ""

    echo -e "${BLUE}Step 2: Building network module...${NC}"
    ./gradlew :network:build -x test --quiet

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Network module built successfully${NC}"
    else
        echo -e "${RED}✗ Failed to build network module${NC}"
        exit 1
    fi
    echo ""
else
    echo -e "${BLUE}Step 1-2: Skipped (--skip-build flag)${NC}"
    echo ""
fi

# Check if JARs exist
echo -e "${BLUE}Step 3: Checking JAR files...${NC}"

if [ ! -f "$COMMON_JAR" ]; then
    echo -e "${RED}✗ Common JAR not found: $COMMON_JAR${NC}"
    echo -e "${YELLOW}  Run without --skip-build to build it${NC}"
    exit 1
fi

if [ ! -f "$NETWORK_JAR" ]; then
    echo -e "${RED}✗ Network JAR not found: $NETWORK_JAR${NC}"
    echo -e "${YELLOW}  Run without --skip-build to build it${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Both JAR files found${NC}"
echo ""

# Display JAR sizes
COMMON_SIZE=$(du -h "$COMMON_JAR" | cut -f1)
NETWORK_SIZE=$(du -h "$NETWORK_JAR" | cut -f1)

echo -e "${BLUE}JAR Information:${NC}"
echo "  common.jar:  $COMMON_SIZE"
echo "  network.jar: $NETWORK_SIZE"
echo ""

# Copy JARs
echo -e "${BLUE}Step 4: Copying JARs to consumer libs...${NC}"

cp "$COMMON_JAR" "$CONSUMER_LIBS_DIR/common.jar"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Copied common.jar${NC}"
else
    echo -e "${RED}✗ Failed to copy common.jar${NC}"
    exit 1
fi

cp "$NETWORK_JAR" "$CONSUMER_LIBS_DIR/network.jar"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Copied network.jar${NC}"
else
    echo -e "${RED}✗ Failed to copy network.jar${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✓ Successfully updated consumer libraries!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Rebuild consumer-app: cd consumer-app && ./gradlew clean build"
echo "  2. Or run consumer-app:   cd consumer-app && ./gradlew run"
echo ""
echo -e "${BLUE}Docker users:${NC}"
echo "  If you're building a Docker image, you MUST rebuild with --no-cache:"
echo "  cd consumer-app && docker build --no-cache -t consumer-app:latest ."
echo ""
echo -e "${YELLOW}Why --no-cache?${NC}"
echo "  Docker caches the 'COPY libs libs/' layer. Even though the JARs are updated,"
echo "  Docker won't notice the change without --no-cache or touching build.gradle"
echo ""
