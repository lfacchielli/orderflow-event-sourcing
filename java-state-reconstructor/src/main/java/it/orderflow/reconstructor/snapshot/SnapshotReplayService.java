package it.orderflow.reconstructor.snapshot;

import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.projection.OrderStateProjector;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public final class SnapshotReplayService {

    private final OrderStateProjector projector;

    public SnapshotReplayService(
        OrderStateProjector projector
    ) {
        this.projector = Objects.requireNonNull(
            projector,
            "projector is required"
        );
    }

    public SnapshotReplayResult replay(
        OrderSnapshot snapshot,
        List<OrderEvent> events,
        long targetVersion
    ) {
        Objects.requireNonNull(
            events,
            "events are required"
        );

        if (targetVersion < 1) {
            throw new IllegalArgumentException(
                "targetVersion must be greater than zero"
            );
        }

        OrderState state =
            snapshot == null ? null : snapshot.state();

        long startingVersion =
            snapshot == null
                ? 0
                : snapshot.aggregateVersion();

        if (startingVersion >= targetVersion) {
            throw new IllegalArgumentException(
                "Snapshot version must be lower "
                    + "than target version"
            );
        }

        List<OrderEvent> relevantEvents = events.stream()
            .filter(
                event ->
                    event.aggregateVersion()
                        > startingVersion
            )
            .filter(
                event ->
                    event.aggregateVersion()
                        <= targetVersion
            )
            .sorted(
                Comparator.comparingLong(
                    OrderEvent::aggregateVersion
                )
            )
            .toList();

        int appliedEvents = 0;

        for (OrderEvent event : relevantEvents) {
            state = projector.apply(state, event);
            appliedEvents++;
        }

        if (state == null) {
            throw new IllegalStateException(
                "Replay produced no order state"
            );
        }

        if (state.version() != targetVersion) {
            throw new IllegalStateException(
                "Replay stopped at version "
                    + state.version()
                    + " instead of "
                    + targetVersion
            );
        }

        return new SnapshotReplayResult(
            state,
            startingVersion,
            appliedEvents
        );
    }
}