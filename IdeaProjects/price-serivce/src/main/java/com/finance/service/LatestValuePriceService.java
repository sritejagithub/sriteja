package com.finance.service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Concurrent price service that keeps the latest value for each id.
 */
public class LatestValuePriceService {

    //Stores the latest published prices
    private final Map<String, PriceRecord> latestPrices = new ConcurrentHashMap<>();

    //Stores active batches
    private final Map<UUID, Map<String, PriceRecord>> activeBatches = new ConcurrentHashMap<>();

    //Tracks whether a batch is completed
    private final Map<UUID, AtomicBoolean> batchStatus = new ConcurrentHashMap<>();

    public static class PriceRecord {
        private final String id; // Field to indicate Instrument
        private final long asOf; //Field to indicate date time
        private final Object payload; //Field to indicate Price data

        public PriceRecord(String id, long asOf, Object payload) {
            this.id = id;
            this.asOf = asOf;
            this.payload = payload;
        }

        public String getId() {
            return id;
        }

        public long getAsOf() {
            return asOf;
        }

        public Object getPayload() {
            return payload;
        }
    }

    /**
     * Starts a new batch.
     */
    public UUID startBatch() {
        UUID batchId = UUID.randomUUID();
        activeBatches.put(batchId, new ConcurrentHashMap<String, PriceRecord>());
        batchStatus.put(batchId, new AtomicBoolean(false));
        return batchId;
    }

    /**
     * Uploads records into an active batch.
     * Keeps only the latest timestamp per id.
     */
    public void uploadChunk(UUID batchId, List<PriceRecord> records) {

        Map<String, PriceRecord> staging = activeBatches.get(batchId);
        AtomicBoolean status = batchStatus.get(batchId);

        if (staging == null || status == null || status.get()) {
            throw new IllegalStateException("Batch is not active: " + batchId);
        }

        for (PriceRecord record : records) {
            PriceRecord existing = staging.get(record.getId());

            if (existing == null || record.getAsOf() > existing.getAsOf()) {
                staging.put(record.getId(), record);
            }
        }
    }

    /**
     * Completes a batch and publishes its data.
     * Only newer prices replace older ones.
     */
    public void completeBatch(UUID batchId) {

        Map<String, PriceRecord> staging = activeBatches.get(batchId);
        AtomicBoolean status = batchStatus.get(batchId);

        if (staging == null || status == null || status.get()) {
            throw new IllegalStateException("Batch already completed or invalid: " + batchId);
        }

        //Mark batch as completed
        status.set(true);

        //Merge batch data into the latest price store
        for (Map.Entry<String, PriceRecord> entry : staging.entrySet()) {
            String id = entry.getKey();
            PriceRecord newRecord = entry.getValue();

            PriceRecord existing = latestPrices.get(id);

            if (existing == null || newRecord.getAsOf() > existing.getAsOf()) {
                latestPrices.put(id, newRecord);
            }
        }

        cleanup(batchId);
    }

    /**
     * Cancels a batch.
     */
    public void cancelBatch(UUID batchId) {
        cleanup(batchId);
    }

    /**
     * Returns latest prices for given ids.
     */
    public Map<String, PriceRecord> getLatestPrices(List<String> ids) {

        return ids.stream()
                .map(latestPrices::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(PriceRecord::getId, r -> r));
    }

    /**
     * Removes batch data from memory.
     */
    private void cleanup(UUID batchId) {
        activeBatches.remove(batchId);
        batchStatus.remove(batchId);
    }
}
