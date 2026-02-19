package com.finance.service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Thread-safe in-memory price service.
 * Uses batches to ensure atomic updates and keeps the latest price by asOf time.
 */
public class LatestValuePriceService {

    private final Map<String, PriceRecord> latestPrices = new ConcurrentHashMap<>();

    private final Map<UUID, Map<String, PriceRecord>> activeBatches = new ConcurrentHashMap<>();

    private final Map<UUID, AtomicBoolean> batchStatus = new ConcurrentHashMap<>();

    public record PriceRecord(String id, Instant asOf, Object payload) {}

    /**
     * Creates and initializes a new batch.
     * Returns the generated batch ID.
     */

    public UUID startBatch() {
        UUID batchId = UUID.randomUUID();
        activeBatches.put(batchId, new ConcurrentHashMap<>());
        batchStatus.put(batchId, new AtomicBoolean(false));
        return batchId;
    }

    /**
     * Uploads price records to a batch.
     * keeps only the latest (by asOf) record per id.
     * fails if the batch is inactive.
     */

    public void uploadChunk(UUID batchId, List<PriceRecord> records) {
        Map<String, PriceRecord> staging = activeBatches.get(batchId);
        if (staging == null || batchStatus.get(batchId).get()) {
            throw new IllegalStateException("Batch is not active: " + batchId);
        }

        for (PriceRecord record : records) {
            // Keep latest within the batch itself
            PriceRecord existing = staging.get(record.id());
            if (existing == null || record.asOf().isAfter(existing.asOf())) {
                staging.put(record.id(), record);
            }
        }

    }

    /**
     * Finalizes a batch and publishes its records atomically.
     * Only the latest (by asOf) price per instrument is kept.
     * Consumers will not see partial batch updates.
     */

    public void completeBatch(UUID batchId) {
        Map<String, PriceRecord> staging = activeBatches.get(batchId);
        AtomicBoolean status = batchStatus.get(batchId);

        if (staging == null || status == null || status.get()) {
            throw new IllegalStateException("Batch already completed or does not exist: " + batchId);
        }

        // Mark batch as completed
        status.set(true);

        // Promote staged data to main batch
        for (Map.Entry<String, PriceRecord> entry : staging.entrySet()) {
            String id = entry.getKey();
            PriceRecord newRecord = entry.getValue();
            PriceRecord existing = latestPrices.get(id);
            if (existing == null || newRecord.asOf().isAfter(existing.asOf())) {
                latestPrices.put(id, newRecord);
            }
        }

        cleanup(batchId);

    }

    public void cancelBatch(UUID batchId) {
        cleanup(batchId);
    }

    private void cleanup(UUID batchId) {
        activeBatches.remove(batchId);
        batchStatus.remove(batchId);
    }

    /**
     * Retrieves the latest values from completed batches only.
     */
    public Map<String, PriceRecord> getLatestPrices(List<String> ids) {
        return ids.stream()
                .map(latestPrices::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(PriceRecord::id, r -> r));
    }
}