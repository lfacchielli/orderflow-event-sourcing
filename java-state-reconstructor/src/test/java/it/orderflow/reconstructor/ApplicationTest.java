package it.orderflow.reconstructor;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ApplicationTest {

    @Test
    void returnsApplicationName() {
        assertEquals(
            "OrderFlow Java State Reconstructor",
            Application.applicationName()
        );
    }
}