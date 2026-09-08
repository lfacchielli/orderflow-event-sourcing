package it.orderflow.reconstructor.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ApiSettingsTest {

    @Test
    void createsValidSettings() {
        ApiSettings settings = new ApiSettings(
            "0.0.0.0",
            8081,
            50,
            4
        );

        assertEquals("0.0.0.0", settings.host());
        assertEquals(8081, settings.port());
        assertEquals(50, settings.backlog());
        assertEquals(4, settings.workerThreads());
    }

    @Test
    void rejectsBlankHost() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new ApiSettings(
                " ",
                8081,
                50,
                4
            )
        );
    }

    @Test
    void rejectsInvalidPort() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new ApiSettings(
                "0.0.0.0",
                0,
                50,
                4
            )
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> new ApiSettings(
                "0.0.0.0",
                65536,
                50,
                4
            )
        );
    }

    @Test
    void rejectsInvalidThreadCount() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new ApiSettings(
                "0.0.0.0",
                8081,
                50,
                0
            )
        );
    }
}
