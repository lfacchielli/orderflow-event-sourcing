package it.orderflow.reconstructor.api;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public final class QueryParameters {

    private QueryParameters() {
    }

    public static Map<String, String> parse(
        String rawQuery
    ) {
        if (rawQuery == null || rawQuery.isBlank()) {
            return Map.of();
        }

        return Arrays.stream(rawQuery.split("&"))
            .map(parameter -> parameter.split("=", 2))
            .filter(parts -> parts.length == 2)
            .collect(
                Collectors.toUnmodifiableMap(
                    parts -> decode(parts[0]),
                    parts -> decode(parts[1]),
                    (first, second) -> second
                )
            );
    }

    private static String decode(String value) {
        return URLDecoder.decode(
            value,
            StandardCharsets.UTF_8
        );
    }
}