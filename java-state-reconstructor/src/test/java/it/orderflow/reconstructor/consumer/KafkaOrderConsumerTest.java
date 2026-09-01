package it.orderflow.reconstructor.consumer;

import it.orderflow.reconstructor.processing.OrderEventProcessor;
import it.orderflow.reconstructor.projection.OrderStateProjector;
import it.orderflow.reconstructor.serialization.OrderEventDeserializer;
import it.orderflow.reconstructor.store.InMemoryOrderStateStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaOrderConsumerTest {

    private static final String TOPIC = "order-events";

    @Test
    void consumesAndCommitsValidOrderCreatedEvent()
        throws Exception {

        MockConsumer<String, String> kafka =
            new MockConsumer<>("earliest");

        InMemoryOrderStateStore store =
            new InMemoryOrderStateStore();

        KafkaOrderConsumer consumer = createConsumer(
            kafka,
            store
        );

        TopicPartition partition =
            assignPartition(kafka);

        kafka.addRecord(
            new ConsumerRecord<>(
                TOPIC,
                partition.partition(),
                0,
                "ORD-2001",
                readResource("order-created.json")
            )
        );

        PollResult result = consumer.pollOnce();

        assertEquals(1, result.receivedRecords());
        assertEquals(1, result.processedRecords());
        assertEquals(1, store.size());

        assertEquals(
            1,
            store.findByOrderId("ORD-2001")
                .orElseThrow()
                .version()
        );

        assertEquals(
            1L,
            kafka.committed(Set.of(partition))
                .get(partition)
                .offset()
        );
    }

    @Test
    void doesNotCommitWhenPollIsEmpty() {
        MockConsumer<String, String> kafka =
            new MockConsumer<>("earliest");

        KafkaOrderConsumer consumer = createConsumer(
            kafka,
            new InMemoryOrderStateStore()
        );

        TopicPartition partition =
            assignPartition(kafka);

        PollResult result = consumer.pollOnce();

        assertEquals(0, result.receivedRecords());
        assertEquals(0, result.processedRecords());

        assertNull(
            kafka.committed(Set.of(partition))
                .get(partition)
        );
    }

    @Test
    void rejectsKafkaKeyDifferentFromAggregateId()
        throws Exception {

        MockConsumer<String, String> kafka =
            new MockConsumer<>("earliest");

        KafkaOrderConsumer consumer = createConsumer(
            kafka,
            new InMemoryOrderStateStore()
        );

        TopicPartition partition =
            assignPartition(kafka);

        kafka.addRecord(
            new ConsumerRecord<>(
                TOPIC,
                partition.partition(),
                0,
                "ORD-WRONG",
                readResource("order-created.json")
            )
        );

        KafkaRecordValidationException exception =
            assertThrows(
                KafkaRecordValidationException.class,
                consumer::pollOnce
            );

        assertEquals(
            "ORD-WRONG",
            exception.recordKey()
        );
        assertEquals(
            "ORD-2001",
            exception.aggregateId()
        );
        assertEquals(
            partition.partition(),
            exception.partition()
        );
        assertEquals(0, exception.offset());

        assertNull(
            kafka.committed(Set.of(partition))
                .get(partition)
        );
    }

    private TopicPartition assignPartition(
        MockConsumer<String, String> kafka
    ) {
        TopicPartition partition =
            new TopicPartition(TOPIC, 0);

        kafka.rebalance(List.of(partition));

        kafka.updateBeginningOffsets(
            Map.of(partition, 0L)
        );

        kafka.updateEndOffsets(
            Map.of(partition, 1L)
        );

        return partition;
    }

    private KafkaOrderConsumer createConsumer(
        MockConsumer<String, String> kafka,
        InMemoryOrderStateStore store
    ) {
        return new KafkaOrderConsumer(
            kafka,
            new KafkaConsumerSettings(
                "unused:9092",
                "test-group",
                "test-consumer",
                TOPIC,
                Duration.ofMillis(10)
            ),
            new OrderEventDeserializer(),
            new OrderEventProcessor(
                store,
                new OrderStateProjector()
            )
        );
    }

    private String readResource(String name)
        throws Exception {

        var resource = getClass()
            .getClassLoader()
            .getResource(name);

        if (resource == null) {
            throw new IllegalStateException(
                "Resource not found: " + name
            );
        }

        return Files.readString(
            Path.of(resource.toURI())
        );
    }
}