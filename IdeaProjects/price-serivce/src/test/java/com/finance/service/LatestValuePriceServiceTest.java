package com.finance.service;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class LatestValuePriceServiceTest {

    @Test
    void testRequirements() {

        //Create service and timestamp
        LatestValuePriceService service = new LatestValuePriceService();
        long now = System.currentTimeMillis();

        //Batch 1
        UUID batch1 = service.startBatch();

        //Upload price but do not complete
        service.uploadChunk(batch1, Arrays.asList(
                new LatestValuePriceService.PriceRecord("BTC", now, "60k")
        ));

        //Incomplete batch should not be visible
        assertTrue(service.getLatestPrices(Arrays.asList("BTC")).isEmpty());

        //Complete batch
        service.completeBatch(batch1);

        //Completed batch data should be visible
        assertEquals("60k",
                service.getLatestPrices(Arrays.asList("BTC"))
                        .get("BTC")
                        .getPayload());

        //Batch 2 - Older price
        UUID batch2 = service.startBatch();

        //Upload older price for same instrument
        service.uploadChunk(batch2, Arrays.asList(
                new LatestValuePriceService.PriceRecord("BTC", now - 100_000, "50k")
        ));

        service.completeBatch(batch2);

        //Older price should not replace newer one
        assertEquals("60k",
                service.getLatestPrices(Arrays.asList("BTC"))
                        .get("BTC")
                        .getPayload());
    }
}
