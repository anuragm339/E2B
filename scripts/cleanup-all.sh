#!/bin/bash
# Complete cleanup script for messaging system
# Removes ALL Docker resources and build artifacts

set -e

echo "=========================================="
echo "  Complete Cleanup - Messaging System"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}WARNING: This will remove:${NC}"
echo "  - All containers (running and stopped)"
echo "  - All Docker images for this project"
echo "  - All Docker volumes (including data)"
echo "  - All Docker networks"
echo "  - All Gradle build artifacts"
echo ""
read -p "Are you sure you want to continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Cleanup cancelled."
    exit 0
fi

echo ""
echo -e "${GREEN}Step 1: Stopping all containers...${NC}"
docker compose down -v --remove-orphans 2>&1 | grep -v "^time=" || true

echo ""
echo -e "${GREEN}Step 2: Removing all project containers (even orphaned ones)...${NC}"
docker ps -a --filter "name=consumer-" --filter "name=broker" --filter "name=cloud-server" --filter "name=prometheus" --filter "name=grafana" --format "{{.Names}}" | xargs -r docker rm -f 2>&1 || true

echo ""
echo -e "${GREEN}Step 3: Removing all project images...${NC}"
docker images --format "{{.Repository}}:{{.Tag}}" | grep -E "(messaging|consumer|cloud|broker)" | xargs -r docker rmi -f 2>&1 || true

echo ""
echo -e "${GREEN}Step 4: Removing all project volumes...${NC}"
docker volume ls --format "{{.Name}}" | grep "messaging" | xargs -r docker volume rm 2>&1 || true

echo ""
echo -e "${GREEN}Step 5: Removing project networks...${NC}"
docker network ls --format "{{.Name}}" | grep "messaging" | xargs -r docker network rm 2>&1 || true

echo ""
echo -e "${GREEN}Step 6: Pruning unused Docker resources...${NC}"
docker system prune -f --volumes

echo ""
echo -e "${GREEN}Step 7: Cleaning Gradle build artifacts...${NC}"
if [ -d "provider" ]; then
    cd provider
    ./gradlew clean 2>&1 | tail -3 || true
    cd ..
fi

if [ -d "cloud-server" ]; then
    cd cloud-server
    ./gradlew clean 2>&1 | tail -3 || true
    cd ..
fi

if [ -d "consumer-app" ]; then
    cd consumer-app
    ./gradlew clean 2>&1 | tail -3 || true
    cd ..
fi

echo ""
echo -e "${GREEN}Step 8: Removing local data directories...${NC}"
rm -rf provider/broker/data-local/* 2>&1 || true
rm -rf data/* 2>&1 || true

echo ""
echo -e "${GREEN}✅ Cleanup complete!${NC}"
echo ""
echo "Summary:"
docker ps -a | wc -l | xargs -I {} echo "  - Containers: {} (should be 1 - header line only)"
docker images | grep -E "(messaging|consumer|cloud|broker)" | wc -l | xargs -I {} echo "  - Project images: {}"
docker volume ls | grep "messaging" | wc -l | xargs -I {} echo "  - Project volumes: {}"
docker network ls | grep "messaging" | wc -l | xargs -I {} echo "  - Project networks: {}"
echo ""
echo "You can now rebuild with: docker compose build"
