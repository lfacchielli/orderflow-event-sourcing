package it.orderflow.reconstructor.replay;

import it.orderflow.reconstructor.domain.OrderState;

import java.util.Objects;

public record ReplayResult(
    OrderState state,
    int appliedEvents,
    int availableEvents
) {

    public ReplayResult {
        Objects.requireNonNull(state, "state is required");

        if (appliedEvents < 1) {
            throw new IllegalArgumentException(
                "appliedEvents must be greater than zero"
            );
        }

        if (availableEvents < appliedEvents) {
            throw new IllegalArgumentException(
                "availableEvents cannot be lower than appliedEvents"
            );
        }
    }

    public boolean isCompleteReplay() {
        return appliedEvents == availableEvents;
    }
}