package it.orderflow.reconstructor.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class OrderHistoryApiPathTest {

    @Test
    void extractsOrderIdFromSnapshotsPath() {
        assertEquals(
            "ORD-2001",
            OrderHistoryApiPath.snapshotOrderId(
                "/api/orders/ORD-2001/snapshots"
            )
        );
    }

    @Test
    void extractsOrderIdFromStatePath() {
        assertEquals(
            "ORD-2001",
            OrderHistoryApiPath.stateOrderId(
                "/api/orders/ORD-2001/state"
            )
        );
    }

    @Test
    void rejectsWrongSuffix() {
        assertNull(
            OrderHistoryApiPath.snapshotOrderId(
                "/api/orders/ORD-2001/state"
            )
        );

        assertNull(
            OrderHistoryApiPath.stateOrderId(
                "/api/orders/ORD-2001/snapshots"
            )
        );
    }

    @Test
    void rejectsMissingOrderId() {
        assertNull(
            OrderHistoryApiPath.snapshotOrderId(
                "/api/orders//snapshots"
            )
        );

        assertNull(
            OrderHistoryApiPath.stateOrderId(
                "/api/orders//state"
            )
        );
    }
}