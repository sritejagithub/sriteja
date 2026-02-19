package com.finance.service;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class LatestValuePriceServiceTest {

    @Test
    void testRequirements() {

        // Create service and timestamp
        LatestValuePriceService service = new LatestValuePriceService();
        Instant now = Instant.now();

        //Batch 1
        UUID batch1 = service.startBatch();

        //Upload price but do not complete
        service.uploadChunk(batch1, List.of(
                new LatestValuePriceService.PriceRecord("BTC", now, "60k")
        ));

        //Incomplete batch should not be visible
        assertTrue(service.getLatestPrices(List.of("BTC")).isEmpty());

        //Complete batch
        service.completeBatch(batch1);

        //Completed batch data should be visible
        assertEquals("60k",
                service.getLatestPrices(List.of("BTC")).get("BTC").payload());

        //Batch 2 - Older price
        UUID batch2 = service.startBatch();

        // Upload older price for same instrument
        service.uploadChunk(batch2, List.of(
                new LatestValuePriceService.PriceRecord("BTC", now.minusSeconds(100), "50k")
        ));

        service.completeBatch(batch2);

        // Older price should not replace newer one
        assertEquals("60k",
                service.getLatestPrices(List.of("BTC")).get("BTC").payload());
    }

}