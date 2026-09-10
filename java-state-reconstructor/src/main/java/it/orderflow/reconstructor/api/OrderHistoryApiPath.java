package it.orderflow.reconstructor.api;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public final class OrderHistoryApiPath {

    private static final String BASE_PATH =
        "/api/orders/";

    private static final String SNAPSHOTS_SUFFIX =
        "/snapshots";

    private static final String STATE_SUFFIX =
        "/state";

    private OrderHistoryApiPath() {
    }

    public static String snapshotOrderId(String path) {
        return extractOrderId(
            path,
            SNAPSHOTS_SUFFIX
        );
    }

    public static String stateOrderId(String path) {
        return extractOrderId(
            path,
            STATE_SUFFIX
        );
    }

    private static String extractOrderId(
        String path,
        String suffix
    ) {
        if (
            path == null
                || !path.startsWith(BASE_PATH)
                || !path.endsWith(suffix)
        ) {
            return null;
        }

        int orderIdStart = BASE_PATH.length();
        int orderIdEnd =
            path.length() - suffix.length();

        if (orderIdEnd <= orderIdStart) {
            return null;
        }

        String encodedOrderId = path.substring(
            orderIdStart,
            orderIdEnd
        );

        if (
            encodedOrderId.isBlank()
                || encodedOrderId.contains("/")
        ) {
            return null;
        }

        String orderId = URLDecoder.decode(
            encodedOrderId,
            StandardCharsets.UTF_8
        );

        return orderId.isBlank()
            ? null
            : orderId;
    }
}