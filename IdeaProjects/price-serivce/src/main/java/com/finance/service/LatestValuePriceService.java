package com.finance.service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Thread-safe, in-memory implementation of the Price Service.
 * Ensures atomicity via staging areas and the "asOf" rule via Map merges.
 */
public class LatestValuePriceService {

    // Main store: Instrument ID -> Latest PriceRecord
    private final Map<String, PriceRecord> latestPrices = new ConcurrentHashMap<>();

    // Staging: Batch ID -> (Instrument ID -> PriceRecord)
    private final Map<UUID, Map<String, PriceRecord>> activeBatches = new ConcurrentHashMap<>();

    // Lifecycle: Tracks if a batch has been finalized to prevent double-completion
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
            // Internal batch consistency: keep latest within the batch itself
            staging.merge(record.id(), record, (oldR, newR) ->
                    newR.asOf().isAfter(oldR.asOf()) ? newR : oldR);
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

        if (staging == null || status == null || !status.compareAndSet(false, true)) {
            throw new IllegalStateException("Batch already completed or does not exist: " + batchId);
        }

        // Atomic "Promotion" to the main store using the asOf rule
        staging.forEach((id, record) ->
                latestPrices.merge(id, record, (oldVal, newVal) ->
                        newVal.asOf().isAfter(oldVal.asOf()) ? newVal : oldVal)
        );

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