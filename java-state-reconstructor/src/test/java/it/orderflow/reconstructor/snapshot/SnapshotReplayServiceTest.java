package it.orderflow.reconstructor.snapshot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.projection.OrderStateProjector;
import it.orderflow.reconstructor.serialization.OrderEventDeserializer;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SnapshotReplayServiceTest {

    private final OrderStateProjector projector =
        new OrderStateProjector();

    private final SnapshotReplayService replayService =
        new SnapshotReplayService(projector);

    @Test
    void replayFromSnapshotMatchesFullReplay()
        throws Exception {

        List<OrderEvent> events = readEvents();

        SnapshotReplayResult fullReplay =
            replayService.replay(
                null,
                events,
                10
            );

        OrderState stateAtVersionFive = null;

        for (int index = 0; index < 5; index++) {
            stateAtVersionFive = projector.apply(
                stateAtVersionFive,
                events.get(index)
            );
        }

        OrderSnapshot snapshot = new OrderSnapshot(
            stateAtVersionFive.orderId(),
            stateAtVersionFive.version(),
            stateAtVersionFive,
            Instant.parse("2026-09-06T10:00:00Z")
        );

        SnapshotReplayResult snapshotReplay =
            replayService.replay(
                snapshot,
                events,
                10
            );

        assertEquals(
            fullReplay.state(),
            snapshotReplay.state()
        );

        assertEquals(0, fullReplay.startingVersion());
        assertEquals(10, fullReplay.appliedEvents());

        assertEquals(
            5,
            snapshotReplay.startingVersion()
        );
        assertEquals(
            5,
            snapshotReplay.appliedEvents()
        );
    }

    @Test
    void reconstructsIntermediateVersionFromScratch()
        throws Exception {

        SnapshotReplayResult result =
            replayService.replay(
                null,
                readEvents(),
                5
            );

        assertEquals(5, result.state().version());
        assertEquals("PACKED", result.state().status().name());
        assertEquals(5, result.appliedEvents());
    }

    @Test
    void rejectsSnapshotAtTargetVersion()
        throws Exception {

        List<OrderEvent> events = readEvents();
        OrderState state = null;

        for (OrderEvent event : events) {
            state = projector.apply(state, event);
        }

        OrderSnapshot snapshot = new OrderSnapshot(
            state.orderId(),
            state.version(),
            state,
            Instant.now()
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> replayService.replay(
                snapshot,
                events,
                10
            )
        );
    }

    private List<OrderEvent> readEvents()
        throws Exception {

        var resource = getClass()
            .getClassLoader()
            .getResource(
                "successful-delivery-with-delay.json"
            );

        if (resource == null) {
            throw new IllegalStateException(
                "Scenario resource not found"
            );
        }

        JsonNode eventsNode = new ObjectMapper()
            .readTree(
                Files.readString(
                    Path.of(resource.toURI())
                )
            )
            .get("events");

        OrderEventDeserializer deserializer =
            new OrderEventDeserializer();

        List<OrderEvent> events = new ArrayList<>();

        for (JsonNode eventNode : eventsNode) {
            events.add(
                deserializer.deserialize(
                    eventNode.toString()
                )
            );
        }

        return List.copyOf(events);
    }
}