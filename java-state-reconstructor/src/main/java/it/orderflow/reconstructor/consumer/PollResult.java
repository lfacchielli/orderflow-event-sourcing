package it.orderflow.reconstructor.consumer;

public record PollResult(
    int receivedRecords,
    int processedRecords
) {
}