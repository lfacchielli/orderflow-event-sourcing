package it.orderflow.reconstructor.snapshot;

import it.orderflow.reconstructor.domain.Destination;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.domain.OrderStatus;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrderSnapshotTest {

    @Test
    void createsSnapshotForMatchingState() {
        OrderState state = state("ORD-2001", 10);

        OrderSnapshot snapshot = new OrderSnapshot(
            "ORD-2001",
            10,
            state,
            Instant.parse("2026-09-06T10:00:00Z")
        );

        assertEquals(
            "ORD-2001",
            snapshot.orderId()
        );
        assertEquals(
            10,
            snapshot.aggregateVersion()
        );
        assertEquals(
            state,
            snapshot.state()
        );
    }

    @Test
    void rejectsDifferentOrderId() {
        OrderState state = state("ORD-2001", 10);

        assertThrows(
            IllegalArgumentException.class,
            () -> new OrderSnapshot(
                "ORD-9999",
                10,
                state,
                Instant.now()
            )
        );
    }

    @Test
    void rejectsDifferentVersion() {
        OrderState state = state("ORD-2001", 10);

        assertThrows(
            IllegalArgumentException.class,
            () -> new OrderSnapshot(
                "ORD-2001",
                9,
                state,
                Instant.now()
            )
        );
    }

    private OrderState state(
        String orderId,
        long version
    ) {
        Instant timestamp =
            Instant.parse("2026-08-29T16:00:00Z");

        return new OrderState(
            orderId,
            OrderStatus.DELIVERED,
            version,
            "CUS-501",
            "EUR",
            List.of(),
            new BigDecimal("55.00"),
            new Destination("Firenze", "IT"),
            "HUB-FIRENZE",
            List.of(
                "HUB-MODENA",
                "HUB-BOLOGNA",
                "HUB-FIRENZE"
            ),
            35,
            true,
            Instant.parse("2026-08-29T10:00:00Z"),
            timestamp,
            timestamp
        );
    }
}