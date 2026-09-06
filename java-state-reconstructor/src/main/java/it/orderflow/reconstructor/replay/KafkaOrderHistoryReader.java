package it.orderflow.reconstructor.replay;

import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.serialization.EventDeserializationException;
import it.orderflow.reconstructor.serialization.OrderEventDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class KafkaOrderHistoryReader {

    private final Consumer<String, String> consumer;
    private final OrderEventDeserializer deserializer;
    private final String topic;
    private final Duration pollTimeout;

    public KafkaOrderHistoryReader(
        Consumer<String, String> consumer,
        OrderEventDeserializer deserializer,
        String topic,
        Duration pollTimeout
    ) {
        this.consumer = Objects.requireNonNull(
            consumer,
            "consumer is required"
        );
        this.deserializer = Objects.requireNonNull(
            deserializer,
            "deserializer is required"
        );
        this.topic = requireText(topic, "topic");
        this.pollTimeout = Objects.requireNonNull(
            pollTimeout,
            "pollTimeout is required"
        );

        if (
            pollTimeout.isZero()
                || pollTimeout.isNegative()
        ) {
            throw new IllegalArgumentException(
                "pollTimeout must be positive"
            );
        }
    }

    public List<OrderEvent> readHistory(
        String orderId
    ) {
        String requiredOrderId =
            requireText(orderId, "orderId");

        List<TopicPartition> partitions =
            consumer.partitionsFor(topic)
                .stream()
                .map(
                    partitionInfo ->
                        new TopicPartition(
                            partitionInfo.topic(),
                            partitionInfo.partition()
                        )
                )
                .toList();

        if (partitions.isEmpty()) {
            throw new ReplayException(
                "Topic has no partitions: " + topic
            );
        }

        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        Map<TopicPartition, Long> endOffsets =
            consumer.endOffsets(partitions);

        List<OrderEvent> history = new ArrayList<>();

        while (!allEndOffsetsReached(
            partitions,
            endOffsets
        )) {
            ConsumerRecords<String, String> records =
                consumer.poll(pollTimeout);

            for (
                ConsumerRecord<String, String> record
                    : records
            ) {
                if (
                    !requiredOrderId.equals(
                        record.key()
                    )
                ) {
                    continue;
                }

                try {
                    OrderEvent event =
                        deserializer.deserialize(
                            record.value()
                        );

                    if (
                        !requiredOrderId.equals(
                            event.aggregateId()
                        )
                    ) {
                        throw new ReplayException(
                            "Kafka key does not match "
                                + "event aggregateId at "
                                + record.topic()
                                + "-"
                                + record.partition()
                                + "@"
                                + record.offset()
                        );
                    }

                    history.add(event);
                } catch (
                    EventDeserializationException exception
                ) {
                    throw new ReplayException(
                        "Unable to deserialize event at "
                            + record.topic()
                            + "-"
                            + record.partition()
                            + "@"
                            + record.offset(),
                        exception
                    );
                }
            }
        }

        history.sort(
            Comparator.comparingLong(
                OrderEvent::aggregateVersion
            )
        );

        return List.copyOf(history);
    }

    private boolean allEndOffsetsReached(
        List<TopicPartition> partitions,
        Map<TopicPartition, Long> endOffsets
    ) {
        for (TopicPartition partition : partitions) {
            long currentPosition =
                consumer.position(partition);

            long endingPosition =
                endOffsets.get(partition);

            if (currentPosition < endingPosition) {
                return false;
            }
        }

        return true;
    }

    private static String requireText(
        String value,
        String fieldName
    ) {
        Objects.requireNonNull(
            value,
            fieldName + " is required"
        );

        if (value.isBlank()) {
            throw new IllegalArgumentException(
                fieldName + " cannot be blank"
            );
        }

        return value;
    }
}