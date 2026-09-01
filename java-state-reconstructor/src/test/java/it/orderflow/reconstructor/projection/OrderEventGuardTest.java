package it.orderflow.reconstructor.projection;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import it.orderflow.reconstructor.domain.Destination;
import it.orderflow.reconstructor.domain.EventType;
import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.domain.OrderStatus;
import it.orderflow.reconstructor.domain.ProducerType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrderEventGuardTest {

    private final OrderEventGuard guard =
        new OrderEventGuard();

    @Test
    void acceptsNextVersionForSameAggregate() {
        guard.validate(
            state(OrderStatus.PAID, 3),
            event("ORD-2001", 4, EventType.INVENTORY_RESERVED)
        );
    }

    @Test
    void rejectsAggregateMismatch() {
        StateProjectionException exception =
            assertThrows(
                StateProjectionException.class,
                () -> guard.validate(
                    state(OrderStatus.PAID, 3),
                    event(
                        "ORD-9000",
                        4,
                        EventType.INVENTORY_RESERVED
                    )
                )
            );

        assertEquals(
            ProjectionErrorCode.AGGREGATE_MISMATCH,
            exception.errorCode()
        );
        assertEquals("ORD-9000", exception.aggregateId());
        assertEquals(3, exception.currentVersion());
        assertEquals(4, exception.eventVersion());
    }

    @Test
    void rejectsSameVersionAsDuplicateOrOld() {
        StateProjectionException exception =
            assertThrows(
                StateProjectionException.class,
                () -> guard.validate(
                    state(OrderStatus.PAID, 3),
                    event(
                        "ORD-2001",
                        3,
                        EventType.PAYMENT_COMPLETED
                    )
                )
            );

        assertEquals(
            ProjectionErrorCode.OLD_OR_DUPLICATE_EVENT,
            exception.errorCode()
        );
    }

    @Test
    void rejectsOlderVersion() {
        StateProjectionException exception =
            assertThrows(
                StateProjectionException.class,
                () -> guard.validate(
                    state(OrderStatus.PAID, 3),
                    event(
                        "ORD-2001",
                        2,
                        EventType.ORDER_CONFIRMED
                    )
                )
            );

        assertEquals(
            ProjectionErrorCode.OLD_OR_DUPLICATE_EVENT,
            exception.errorCode()
        );
    }

    @Test
    void rejectsVersionGap() {
        StateProjectionException exception =
            assertThrows(
                StateProjectionException.class,
                () -> guard.validate(
                    state(OrderStatus.PAID, 3),
                    event(
                        "ORD-2001",
                        5,
                        EventType.ORDER_PACKED
                    )
                )
            );

        assertEquals(
            ProjectionErrorCode.VERSION_GAP,
            exception.errorCode()
        );
        assertEquals(3, exception.currentVersion());
        assertEquals(5, exception.eventVersion());
        assertEquals(
            "Expected event version 4 but received 5",
            exception.getMessage()
        );
    }

    @Test
    void rejectsNewEventAfterDelivery() {
        StateProjectionException exception =
            assertThrows(
                StateProjectionException.class,
                () -> guard.validate(
                    state(OrderStatus.DELIVERED, 10),
                    event(
                        "ORD-2001",
                        11,
                        EventType.HUB_REACHED
                    )
                )
            );

        assertEquals(
            ProjectionErrorCode.TERMINAL_STATE,
            exception.errorCode()
        );
        assertEquals(
            OrderStatus.DELIVERED,
            exception.currentStatus()
        );
    }

    @Test
    void rejectsNewEventAfterCancellation() {
        StateProjectionException exception =
            assertThrows(
                StateProjectionException.class,
                () -> guard.validate(
                    state(OrderStatus.CANCELLED, 3),
                    event(
                        "ORD-2001",
                        4,
                        EventType.PAYMENT_COMPLETED
                    )
                )
            );

        assertEquals(
            ProjectionErrorCode.TERMINAL_STATE,
            exception.errorCode()
        );
    }

    @Test
    void versionValidationPrecedesTerminalValidation() {
        StateProjectionException exception =
            assertThrows(
                StateProjectionException.class,
                () -> guard.validate(
                    state(OrderStatus.DELIVERED, 10),
                    event(
                        "ORD-2001",
                        10,
                        EventType.ORDER_DELIVERED
                    )
                )
            );

        assertEquals(
            ProjectionErrorCode.OLD_OR_DUPLICATE_EVENT,
            exception.errorCode()
        );
    }

    private OrderState state(
        OrderStatus status,
        long version
    ) {
        Instant timestamp =
            Instant.parse("2026-08-29T10:00:00Z");

        return new OrderState(
            "ORD-2001",
            status,
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
            status == OrderStatus.DELIVERED
                ? timestamp
                : null,
            timestamp
        );
    }

    private OrderEvent event(
        String aggregateId,
        long version,
        EventType eventType
    ) {
        return new OrderEvent(
            UUID.randomUUID(),
            eventType,
            aggregateId,
            version,
            Instant.parse("2026-08-29T11:00:00Z"),
            "test-node",
            ProducerType.ECOMMERCE,
            aggregateId.replace("ORD-", "CORR-"),
            JsonNodeFactory.instance.objectNode()
        );
    }
}