package it.orderflow.reconstructor.snapshot;

import it.orderflow.reconstructor.domain.OrderState;

import java.util.Objects;

public final class SnapshotPolicy {

    private final long interval;

    public SnapshotPolicy(long interval) {
        if (interval < 1) {
            throw new IllegalArgumentException(
                "Snapshot interval must be greater than zero"
            );
        }

        this.interval = interval;
    }

    public long interval() {
        return interval;
    }

    public boolean shouldCreateSnapshot(
        OrderState state
    ) {
        Objects.requireNonNull(
            state,
            "state is required"
        );

        return state.version() % interval == 0;
    }
}