package com.finance.service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Thread-safe in-memory price service.
 * Batch updates are published in a single step,
 * and the most recent price (by asOf) is retained.
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
     * Adds price records to an active batch.
     * Retains only the most recent record per instrument.
     * Throws an exception if the batch is not active.
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
     * Marks a batch as complete and publishes its prices.
     * Retains the latest value by asOf time.
     */
    public void completeBatch(UUID batchId) {
        Map<String, PriceRecord> staging = activeBatches.get(batchId);
        AtomicBoolean status = batchStatus.get(batchId);

        if (staging == null || status == null || status.get()) {
            throw new IllegalStateException("Batch already completed or does not exist: " + batchId);
        }

        //Mark batch as completed
        status.set(true);

        //Move staged data to the main price map
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