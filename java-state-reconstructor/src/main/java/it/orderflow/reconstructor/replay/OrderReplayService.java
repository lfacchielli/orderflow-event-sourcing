package it.orderflow.reconstructor.replay;

import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.projection.OrderStateProjector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public final class OrderReplayService {

    private final OrderStateProjector projector;

    public OrderReplayService(
        OrderStateProjector projector
    ) {
        this.projector = Objects.requireNonNull(
            projector,
            "projector is required"
        );
    }

    public ReplayResult replayAll(
        List<OrderEvent> events
    ) {
        List<OrderEvent> orderedEvents =
            validateAndOrder(events);

        return replaySelected(
            orderedEvents,
            event -> true
        );
    }

    public ReplayResult replayToVersion(
        List<OrderEvent> events,
        long targetVersion
    ) {
        if (targetVersion < 1) {
            throw new IllegalArgumentException(
                "targetVersion must be greater than zero"
            );
        }

        List<OrderEvent> orderedEvents =
            validateAndOrder(events);

        long finalVersion = orderedEvents
            .get(orderedEvents.size() - 1)
            .aggregateVersion();

        if (targetVersion > finalVersion) {
            throw new ReplayException(
                "Target version "
                    + targetVersion
                    + " is greater than final version "
                    + finalVersion
            );
        }

        return replaySelected(
            orderedEvents,
            event ->
                event.aggregateVersion() <= targetVersion
        );
    }

    public ReplayResult replayAtTime(
        List<OrderEvent> events,
        Instant targetTime
    ) {
        Objects.requireNonNull(
            targetTime,
            "targetTime is required"
        );

        List<OrderEvent> orderedEvents =
            validateAndOrder(events);

        Instant creationTime = orderedEvents
            .get(0)
            .occurredAt();

        if (targetTime.isBefore(creationTime)) {
            throw new ReplayException(
                "Target time "
                    + targetTime
                    + " is before order creation time "
                    + creationTime
            );
        }

        return replaySelected(
            orderedEvents,
            event ->
                !event.occurredAt().isAfter(targetTime)
        );
    }

    private ReplayResult replaySelected(
        List<OrderEvent> orderedEvents,
        Predicate<OrderEvent> selection
    ) {
        OrderState state = null;
        int appliedEvents = 0;

        for (OrderEvent event : orderedEvents) {
            if (!selection.test(event)) {
                continue;
            }

            state = projector.apply(state, event);
            appliedEvents++;
        }

        if (state == null) {
            throw new ReplayException(
                "No event could be applied during replay"
            );
        }

        return new ReplayResult(
            state,
            appliedEvents,
            orderedEvents.size()
        );
    }

    private List<OrderEvent> validateAndOrder(
        List<OrderEvent> events
    ) {
        Objects.requireNonNull(
            events,
            "events are required"
        );

        if (events.isEmpty()) {
            throw new ReplayException(
                "Replay requires at least one event"
            );
        }

        if (events.stream().anyMatch(Objects::isNull)) {
            throw new ReplayException(
                "Replay events cannot contain null values"
            );
        }

        List<OrderEvent> orderedEvents =
            new ArrayList<>(events);

        orderedEvents.sort(
            Comparator.comparingLong(
                OrderEvent::aggregateVersion
            )
        );

        String aggregateId = orderedEvents
            .get(0)
            .aggregateId();

        boolean aggregateMismatch = orderedEvents
            .stream()
            .anyMatch(
                event ->
                    !aggregateId.equals(
                        event.aggregateId()
                    )
            );

        if (aggregateMismatch) {
            throw new ReplayException(
                "Replay events must belong to one aggregate"
            );
        }

        long expectedVersion = 1;

        for (OrderEvent event : orderedEvents) {
            if (
                event.aggregateVersion()
                    != expectedVersion
            ) {
                throw new ReplayException(
                    "Expected version "
                        + expectedVersion
                        + " but found "
                        + event.aggregateVersion()
                );
            }

            expectedVersion++;
        }

        for (
            int index = 1;
            index < orderedEvents.size();
            index++
        ) {
            Instant previousTimestamp = orderedEvents
                .get(index - 1)
                .occurredAt();

            Instant currentTimestamp = orderedEvents
                .get(index)
                .occurredAt();

            if (
                currentTimestamp.isBefore(
                    previousTimestamp
                )
            ) {
                throw new ReplayException(
                    "Event timestamps are not ordered "
                        + "by aggregate version"
                );
            }
        }

        return List.copyOf(orderedEvents);
    }
}