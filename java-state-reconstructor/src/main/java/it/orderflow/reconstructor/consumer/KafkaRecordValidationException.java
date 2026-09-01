package it.orderflow.reconstructor.consumer;

public final class KafkaRecordValidationException
    extends RuntimeException {

    private final String topic;
    private final int partition;
    private final long offset;
    private final String recordKey;
    private final String aggregateId;

    public KafkaRecordValidationException(
        String topic,
        int partition,
        long offset,
        String recordKey,
        String aggregateId
    ) {
        super(
            "Kafka record key "
                + recordKey
                + " does not match aggregateId "
                + aggregateId
        );

        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.recordKey = recordKey;
        this.aggregateId = aggregateId;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public String recordKey() {
        return recordKey;
    }

    public String aggregateId() {
        return aggregateId;
    }
}