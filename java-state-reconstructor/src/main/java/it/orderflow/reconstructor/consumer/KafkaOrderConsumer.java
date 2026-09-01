package it.orderflow.reconstructor.consumer;

import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.processing.OrderEventProcessor;
import it.orderflow.reconstructor.serialization.OrderEventDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.List;
import java.util.Objects;

public final class KafkaOrderConsumer
    implements AutoCloseable {

    private final Consumer<String, String> consumer;
    private final KafkaConsumerSettings settings;
    private final OrderEventDeserializer deserializer;
    private final OrderEventProcessor processor;

    public KafkaOrderConsumer(
        Consumer<String, String> consumer,
        KafkaConsumerSettings settings,
        OrderEventDeserializer deserializer,
        OrderEventProcessor processor
    ) {
        this.consumer = Objects.requireNonNull(
            consumer,
            "consumer is required"
        );
        this.settings = Objects.requireNonNull(
            settings,
            "settings are required"
        );
        this.deserializer = Objects.requireNonNull(
            deserializer,
            "deserializer is required"
        );
        this.processor = Objects.requireNonNull(
            processor,
            "processor is required"
        );

        this.consumer.subscribe(
            List.of(settings.topic())
        );
    }

    public PollResult pollOnce() {
        ConsumerRecords<String, String> records =
            consumer.poll(settings.pollTimeout());

        int processedRecords = 0;

        for (
            ConsumerRecord<String, String> record
                : records
        ) {
            processRecord(record);
            processedRecords++;
        }

        if (processedRecords > 0) {
            consumer.commitSync();
        }

        return new PollResult(
            records.count(),
            processedRecords
        );
    }

    private void processRecord(
        ConsumerRecord<String, String> record
    ) {
        OrderEvent event = deserializer.deserialize(
            record.value()
        );

        if (
            record.key() == null
                || !record.key().equals(
                    event.aggregateId()
                )
        ) {
            throw new KafkaRecordValidationException(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                event.aggregateId()
            );
        }

        processor.process(event);
    }

    @Override
    public void close() {
        consumer.close();
    }
}