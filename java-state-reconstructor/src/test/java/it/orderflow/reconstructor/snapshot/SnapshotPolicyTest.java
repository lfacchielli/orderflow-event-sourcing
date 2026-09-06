package it.orderflow.reconstructor.snapshot;

import it.orderflow.reconstructor.domain.Destination;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.domain.OrderStatus;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotPolicyTest {

    @Test
    void createsSnapshotAtVersionFive() {
        SnapshotPolicy policy = new SnapshotPolicy(5);

        assertTrue(
            policy.shouldCreateSnapshot(state(5))
        );
    }

    @Test
    void createsSnapshotAtVersionTen() {
        SnapshotPolicy policy = new SnapshotPolicy(5);

        assertTrue(
            policy.shouldCreateSnapshot(state(10))
        );
    }

    @Test
    void doesNotCreateSnapshotBetweenIntervals() {
        SnapshotPolicy policy = new SnapshotPolicy(5);

        assertFalse(
            policy.shouldCreateSnapshot(state(4))
        );
        assertFalse(
            policy.shouldCreateSnapshot(state(6))
        );
        assertFalse(
            policy.shouldCreateSnapshot(state(9))
        );
    }

    @Test
    void rejectsNonPositiveInterval() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new SnapshotPolicy(0)
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> new SnapshotPolicy(-1)
        );
    }

    private OrderState state(long version) {
        Instant timestamp =
            Instant.parse("2026-08-29T10:00:00Z");

        return new OrderState(
            "ORD-2001",
            OrderStatus.CREATED,
            version,
            "CUS-501",
            "EUR",
            List.of(),
            new BigDecimal("55.00"),
            new Destination("Firenze", "IT"),
            null,
            List.of(),
            0,
            false,
            timestamp,
            null,
            timestamp
        );
    }
}