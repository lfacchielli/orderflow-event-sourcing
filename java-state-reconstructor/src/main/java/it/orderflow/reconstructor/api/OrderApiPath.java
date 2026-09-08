package it.orderflow.reconstructor.api;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public final class OrderApiPath {

    private static final String BASE_PATH =
        "/api/orders";

    private OrderApiPath() {
    }

    public static boolean isCollection(String path) {
        return BASE_PATH.equals(path)
            || (BASE_PATH + "/").equals(path);
    }

    public static String extractOrderId(String path) {
        if (
            path == null
                || isCollection(path)
                || !path.startsWith(BASE_PATH + "/")
        ) {
            return null;
        }

        String encodedOrderId = path.substring(
            (BASE_PATH + "/").length()
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