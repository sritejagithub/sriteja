package com.finance.service;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class LatestValuePriceServiceTest {

    @Test
    void testRequirements() {
        LatestValuePriceService service = new LatestValuePriceService();
        Instant now = Instant.now();

        UUID batch1 = service.startBatch();

        service.uploadChunk(batch1, List.of(
                new LatestValuePriceService.PriceRecord("BTC", now, "60k")
        ));

        // Data should NOT be visible before completion
        assertTrue(service.getLatestPrices(List.of("BTC")).isEmpty());

        service.completeBatch(batch1);

        // Now data should be visible
        assertEquals("60k", service.getLatestPrices(List.of("BTC")).get("BTC").payload());

        // Older price
        UUID batch2 = service.startBatch();

        service.uploadChunk(batch2, List.of(
                new LatestValuePriceService.PriceRecord("BTC", now.minusSeconds(100), "50k")
        ));

        service.completeBatch(batch2);

        // Latest value should remain unchanged (asOf rule)
        assertEquals("60k",
                service.getLatestPrices(List.of("BTC")).get("BTC").payload());

    }
}