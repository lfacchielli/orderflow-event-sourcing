package it.orderflow.reconstructor.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrderItemTest {

    @Test
    void calculatesLineTotal() {
        OrderItem item = new OrderItem(
            "PRD-100",
            "Wireless Sensor",
            2,
            new BigDecimal("20.00")
        );

        assertEquals(
            new BigDecimal("40.00"),
            item.lineTotal()
        );
    }

    @Test
    void rejectsInvalidQuantity() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new OrderItem(
                "PRD-100",
                "Wireless Sensor",
                0,
                new BigDecimal("20.00")
            )
        );
    }

    @Test
    void rejectsNegativePrice() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new OrderItem(
                "PRD-100",
                "Wireless Sensor",
                1,
                new BigDecimal("-1.00")
            )
        );
    }
}