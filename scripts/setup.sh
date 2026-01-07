#!/bin/bash

set -e

echo "=========================================="
echo "Social Media Event Platform Setup"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"

command -v docker >/dev/null 2>&1 || { echo -e "${RED}Docker is not installed. Aborting.${NC}" >&2; exit 1; }
command -v docker-compose >/dev/null 2>&1 || { echo -e "${RED}Docker Compose is not installed. Aborting.${NC}" >&2; exit 1; }

echo -e "${GREEN}✓ Docker found${NC}"
echo -e "${GREEN}✓ Docker Compose found${NC}"

# Create .env if it doesn't exist
if [ ! -f .env ]; then
    echo -e "${YELLOW}Creating .env file from template...${NC}"
    cp .env.example .env
    echo -e "${GREEN}✓ .env file created${NC}"
    echo -e "${YELLOW}Please edit .env with your configuration${NC}"
else
    echo -e "${GREEN}✓ .env file exists${NC}"
fi

# Create necessary directories
echo -e "${YELLOW}Creating directories...${NC}"
mkdir -p logs
mkdir -p data/mysql
mkdir -p data/kafka
mkdir -p data/zookeeper
mkdir -p /tmp/spark-checkpoints
echo -e "${GREEN}✓ Directories created${NC}"

# Build Docker images
echo -e "${YELLOW}Building Docker images...${NC}"
docker-compose -f infrastructure/docker-compose.yml build
echo -e "${GREEN}✓ Docker images built${NC}"

# Start services
echo -e "${YELLOW}Starting services...${NC}"
docker-compose -f infrastructure/docker-compose.yml up -d
echo -e "${GREEN}✓ Services started${NC}"

# Wait for services to be healthy
echo -e "${YELLOW}Waiting for services to be healthy...${NC}"
sleep 10

# Check health
echo -e "${YELLOW}Checking service health...${NC}"

check_health() {
    local url=$1
    local service=$2
    
    if curl -f -s "$url" > /dev/null; then
        echo -e "${GREEN}✓ $service is healthy${NC}"
        return 0
    else
        echo -e "${RED}✗ $service is not responding${NC}"
        return 1
    fi
}

# Wait up to 60 seconds for services
MAX_WAIT=60
WAITED=0

while [ $WAITED -lt $MAX_WAIT ]; do
    if check_health "http://localhost:8000/health" "Ingestion Service" && \
       check_health "http://localhost:8001/health" "Analytics Service"; then
        break
    fi
    
    echo -e "${YELLOW}Waiting for services... ($WAITED/$MAX_WAIT seconds)${NC}"
    sleep 5
    WAITED=$((WAITED + 5))
done

if [ $WAITED -ge $MAX_WAIT ]; then
    echo -e "${RED}Services failed to start within $MAX_WAIT seconds${NC}"
    echo -e "${YELLOW}Check logs with: docker-compose -f infrastructure/docker-compose.yml logs${NC}"
    exit 1
fi

# Create Kafka topics
echo -e "${YELLOW}Creating Kafka topics...${NC}"
./scripts/create_topics.sh
echo -e "${GREEN}✓ Kafka topics created${NC}"

echo ""
echo "=========================================="
echo -e "${GREEN}Setup Complete!${NC}"
echo "=========================================="
echo ""
echo "Services running at:"
echo "  - Ingestion API: http://localhost:8000"
echo "  - Analytics API: http://localhost:8001"
echo "  - Ingestion Docs: http://localhost:8000/docs"
echo "  - Analytics Docs: http://localhost:8001/docs"
echo ""
echo "Useful commands:"
echo "  - View logs: docker-compose -f infrastructure/docker-compose.yml logs -f"
echo "  - Stop services: docker-compose -f infrastructure/docker-compose.yml down"
echo "  - Restart services: docker-compose -f infrastructure/docker-compose.yml restart"
echo ""
echo "Next steps:"
echo "  1. Test ingestion: curl -X POST http://localhost:8000/api/v1/events -H 'Content-Type: application/json' -d '{\"event_type\":\"like\",\"user_id\":\"test_user\",\"platform\":\"web\"}'"
echo "  2. Check analytics: curl http://localhost:8001/api/v1/analytics/events"
echo "  3. Generate test data: python scripts/seed_data.py"
echo ""