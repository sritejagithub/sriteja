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

        UUID batchId = service.startBatch();

        // Test that prices are invisible until the batch is completed
        service.uploadChunk(batchId, List.of(
                new LatestValuePriceService.PriceRecord("BTC", now, "60k")
        ));
        assertTrue(service.getLatestPrices(List.of("BTC")).isEmpty(), "Uncompleted batch should be invisible");

        // Test that completed batch data is published to the latest price store
        service.completeBatch(batchId);
        assertEquals("60k", service.getLatestPrices(List.of("BTC")).get("BTC").payload());

        //Test asOf Logic: Upload an older price in a new batch
        UUID batch2 = service.startBatch();
        service.uploadChunk(batch2, List.of(
                new LatestValuePriceService.PriceRecord("BTC", now.minusSeconds(100), "50k")
        ));
        service.completeBatch(batch2);

        assertEquals("60k", service.getLatestPrices(List.of("BTC")).get("BTC").payload());
    }
}