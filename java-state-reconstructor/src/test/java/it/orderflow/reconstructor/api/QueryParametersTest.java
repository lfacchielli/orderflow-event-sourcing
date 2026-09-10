package it.orderflow.reconstructor.api;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryParametersTest {

    @Test
    void parsesVersionParameter() {
        Map<String, String> parameters =
            QueryParameters.parse("version=5");

        assertEquals("5", parameters.get("version"));
    }

    @Test
    void parsesMultipleParameters() {
        Map<String, String> parameters =
            QueryParameters.parse(
                "version=5&format=full"
            );

        assertEquals("5", parameters.get("version"));
        assertEquals("full", parameters.get("format"));
    }

    @Test
    void decodesValues() {
        Map<String, String> parameters =
            QueryParameters.parse(
                "name=ORD%202001"
            );

        assertEquals(
            "ORD 2001",
            parameters.get("name")
        );
    }

    @Test
    void returnsEmptyMapForMissingQuery() {
        assertTrue(
            QueryParameters.parse(null).isEmpty()
        );
        assertTrue(
            QueryParameters.parse("").isEmpty()
        );
    }
}