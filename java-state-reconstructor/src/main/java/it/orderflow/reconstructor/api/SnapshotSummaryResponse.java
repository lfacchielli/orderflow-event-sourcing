package it.orderflow.reconstructor.api;

import it.orderflow.reconstructor.snapshot.OrderSnapshot;

import java.time.Instant;

public record SnapshotSummaryResponse(
    long version,
    String status,
    Instant stateUpdatedAt,
    Instant snapshotCreatedAt
) {

    public static SnapshotSummaryResponse from(
        OrderSnapshot snapshot
    ) {
        return new SnapshotSummaryResponse(
            snapshot.aggregateVersion(),
            snapshot.state().status().name(),
            snapshot.state().lastUpdatedAt(),
            snapshot.createdAt()
        );
    }
}