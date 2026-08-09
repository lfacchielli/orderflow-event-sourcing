#!/bin/sh
set -e

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"
KAFKA_CONFIGS="/opt/kafka/bin/kafka-configs.sh"

create_topic() {
    topic_name="$1"
    partitions="$2"
    cleanup_policy="$3"

    "$KAFKA_TOPICS" \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --create \
        --if-not-exists \
        --topic "$topic_name" \
        --partitions "$partitions" \
        --replication-factor 1 \
        --config "cleanup.policy=$cleanup_policy"
}

create_topic "order-events" 3 "delete"
create_topic "order-state" 3 "compact"
create_topic "order-events-dlq" 3 "delete"

"$KAFKA_CONFIGS" \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --entity-type topics \
    --entity-name "order-events" \
    --alter \
    --add-config "cleanup.policy=delete,retention.ms=-1,retention.bytes=-1"

echo "OrderFlow Kafka topics are ready."