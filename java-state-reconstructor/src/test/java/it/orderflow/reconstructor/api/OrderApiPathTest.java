package it.orderflow.reconstructor.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderApiPathTest {

    @Test
    void recognizesCollectionPath() {
        assertTrue(
            OrderApiPath.isCollection("/api/orders")
        );
        assertTrue(
            OrderApiPath.isCollection("/api/orders/")
        );
    }

    @Test
    void extractsOrderIdentifier() {
        assertEquals(
            "ORD-2001",
            OrderApiPath.extractOrderId(
                "/api/orders/ORD-2001"
            )
        );
    }

    @Test
    void decodesOrderIdentifier() {
        assertEquals(
            "ORD 2001",
            OrderApiPath.extractOrderId(
                "/api/orders/ORD%202001"
            )
        );
    }

    @Test
    void rejectsInvalidPaths() {
        assertNull(
            OrderApiPath.extractOrderId(
                "/api/orders"
            )
        );
        assertNull(
            OrderApiPath.extractOrderId(
                "/api/orders/ORD-2001/events"
            )
        );
        assertNull(
            OrderApiPath.extractOrderId(
                "/different/path"
            )
        );
    }
}