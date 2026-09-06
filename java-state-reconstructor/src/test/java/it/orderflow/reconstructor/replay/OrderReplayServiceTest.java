package it.orderflow.reconstructor.replay;

import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.domain.OrderStatus;
import it.orderflow.reconstructor.projection.OrderStateProjector;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderReplayServiceTest {

    private final OrderReplayService replayService =
        new OrderReplayService(
            new OrderStateProjector()
        );

    @Test
    void replaysCompleteOrderHistory()
        throws Exception {

        ReplayResult result = replayService.replayAll(
            ReplayTestData.scenarioEvents()
        );

        OrderState state = result.state();

        assertEquals(OrderStatus.DELIVERED, state.status());
        assertEquals(10, state.version());
        assertEquals("HUB-FIRENZE", state.currentHub());
        assertEquals(35, state.totalDelayMinutes());
        assertEquals(10, result.appliedEvents());
        assertEquals(10, result.availableEvents());
        assertTrue(result.isCompleteReplay());
    }

    @Test
    void replaysOrderAtVersionFive()
        throws Exception {

        ReplayResult result =
            replayService.replayToVersion(
                ReplayTestData.scenarioEvents(),
                5
            );

        assertEquals(
            OrderStatus.PACKED,
            result.state().status()
        );
        assertEquals(5, result.state().version());
        assertEquals(5, result.appliedEvents());
        assertFalse(result.isCompleteReplay());
    }

    @Test
    void replaysOrderBeforeDelay()
        throws Exception {

        ReplayResult result =
            replayService.replayAtTime(
                ReplayTestData.scenarioEvents(),
                Instant.parse(
                    "2026-08-29T12:45:00Z"
                )
            );

        assertEquals(
            OrderStatus.IN_TRANSIT,
            result.state().status()
        );
        assertEquals(7, result.state().version());
        assertEquals(
            "HUB-BOLOGNA",
            result.state().currentHub()
        );
        assertEquals(
            0,
            result.state().totalDelayMinutes()
        );
        assertFalse(result.state().hasDelay());
    }

    @Test
    void replaysOrderIncludingDelay()
        throws Exception {

        ReplayResult result =
            replayService.replayAtTime(
                ReplayTestData.scenarioEvents(),
                Instant.parse(
                    "2026-08-29T13:00:00Z"
                )
            );

        assertEquals(
            OrderStatus.IN_TRANSIT,
            result.state().status()
        );
        assertEquals(8, result.state().version());
        assertEquals(
            35,
            result.state().totalDelayMinutes()
        );
        assertTrue(result.state().hasDelay());
    }

    @Test
    void sortsEventsByAggregateVersion()
        throws Exception {

        List<OrderEvent> shuffledEvents =
            new ArrayList<>(
                ReplayTestData.scenarioEvents()
            );

        Collections.reverse(shuffledEvents);

        ReplayResult result =
            replayService.replayAll(shuffledEvents);

        assertEquals(
            OrderStatus.DELIVERED,
            result.state().status()
        );
        assertEquals(10, result.state().version());
    }

    @Test
    void rejectsEmptyHistory() {
        assertThrows(
            ReplayException.class,
            () -> replayService.replayAll(List.of())
        );
    }

    @Test
    void rejectsVersionBeyondHistory()
        throws Exception {

        assertThrows(
            ReplayException.class,
            () -> replayService.replayToVersion(
                ReplayTestData.scenarioEvents(),
                11
            )
        );
    }

    @Test
    void rejectsTimeBeforeOrderCreation()
        throws Exception {

        assertThrows(
            ReplayException.class,
            () -> replayService.replayAtTime(
                ReplayTestData.scenarioEvents(),
                Instant.parse(
                    "2026-08-29T09:59:59Z"
                )
            )
        );
    }
}