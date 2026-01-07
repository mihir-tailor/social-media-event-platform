#!/bin/bash

set -e

KAFKA_CONTAINER="kafka"
BOOTSTRAP_SERVER="localhost:9092"
TOPIC_NAME="social-events"
PARTITIONS=3
REPLICATION_FACTOR=1

echo "Creating Kafka topic: $TOPIC_NAME"

# Check if topic already exists
TOPIC_EXISTS=$(docker exec $KAFKA_CONTAINER kafka-topics \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --list | grep "^${TOPIC_NAME}$" || echo "")

if [ -z "$TOPIC_EXISTS" ]; then
    # Create topic
    docker exec $KAFKA_CONTAINER kafka-topics \
        --create \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $TOPIC_NAME \
        --partitions $PARTITIONS \
        --replication-factor $REPLICATION_FACTOR \
        --config retention.ms=604800000 \
        --config compression.type=snappy
    
    echo "✓ Topic '$TOPIC_NAME' created successfully"
else
    echo "✓ Topic '$TOPIC_NAME' already exists"
fi

# Describe topic
echo ""
echo "Topic details:"
docker exec $KAFKA_CONTAINER kafka-topics \
    --describe \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --topic $TOPIC_NAME
    