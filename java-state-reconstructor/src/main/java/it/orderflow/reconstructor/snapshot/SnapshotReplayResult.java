package it.orderflow.reconstructor.snapshot;

import it.orderflow.reconstructor.domain.OrderState;

import java.util.Objects;

public record SnapshotReplayResult(
    OrderState state,
    long startingVersion,
    int appliedEvents
) {

    public SnapshotReplayResult {
        Objects.requireNonNull(
            state,
            "state is required"
        );

        if (startingVersion < 0) {
            throw new IllegalArgumentException(
                "startingVersion cannot be negative"
            );
        }

        if (appliedEvents < 0) {
            throw new IllegalArgumentException(
                "appliedEvents cannot be negative"
            );
        }
    }
}