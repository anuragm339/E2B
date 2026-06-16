package com.messaging.broker.http;

import com.messaging.broker.compaction.SharedRocksDb;
import com.messaging.common.api.StorageEngine;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import jakarta.inject.Inject;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * ███ TEMPORARY VALIDATION TOOL — DELETE AFTER THE CONSISTENCY FAULT-INJECTION DEMO ███
 *
 * Tampers with the RocksDB compaction index (the consistency check's source of truth) so a
 * live INCONSISTENT verdict can be validated end-to-end without hand-editing binary RocksDB
 * files (which would only trip block checksums):
 *
 *   POST /test/consistency/tamper-index?topic=prices-v1&removeCount=5&addCount=5
 *
 * - removeCount: deletes the first N index entries for the topic → the next check finds the
 *   cloud holding records (≤ watermark, physically present) this node's index lacks
 *   → INCONSISTENT (missingKeys).
 * - addCount: inserts N fabricated keys at the topic's current head offset → the next check
 *   finds keys the authoritative cloud never had → INCONSISTENT (fabricatedKeys).
 *
 * No specs by design — this controller is throwaway.
 */
@Requires(property = "broker.test-endpoints.enabled", value = "true")
@Controller("/test/consistency")
public class ConsistencyTamperTestController {

    private static final Logger log = LoggerFactory.getLogger(ConsistencyTamperTestController.class);

    private final SharedRocksDb sharedDb;
    private final StorageEngine storage;

    @Inject
    public ConsistencyTamperTestController(SharedRocksDb sharedDb, StorageEngine storage) {
        this.sharedDb = sharedDb;
        this.storage = storage;
        log.warn("TEMPORARY ConsistencyTamperTestController is active — delete after validation");
    }

    @Post("/tamper-index")
    @Produces(MediaType.APPLICATION_JSON)
    public HttpResponse<String> tamperIndex(
            @QueryValue String topic,
            @QueryValue(defaultValue = "0") int removeCount,
            @QueryValue(defaultValue = "0") int addCount) {
        try {
            List<String> removed = new ArrayList<>();
            byte[] prefix = (topic + "|").getBytes(StandardCharsets.UTF_8);

            try (RocksIterator iter = sharedDb.getDb().newIterator(sharedDb.getCompactionHandle())) {
                iter.seek(prefix);
                while (iter.isValid() && removed.size() < removeCount) {
                    byte[] key = iter.key();
                    if (!startsWith(key, prefix)) break;
                    sharedDb.getDb().delete(sharedDb.getCompactionHandle(),
                            sharedDb.getWriteOptions(), key);
                    removed.add(new String(key, prefix.length, key.length - prefix.length,
                            StandardCharsets.UTF_8));
                    iter.next();
                }
            }

            List<String> added = new ArrayList<>();
            long head = storage.getCurrentOffset(topic, 0);
            for (int i = 0; i < addCount; i++) {
                String fakeKey = "tampered-key-" + System.currentTimeMillis() + "-" + i;
                ByteBuffer value = ByteBuffer.allocate(16);
                value.putLong(Math.max(0, head));        // within the watermark → enters the digest
                value.putLong(System.currentTimeMillis());
                sharedDb.getDb().put(sharedDb.getCompactionHandle(), sharedDb.getWriteOptions(),
                        (topic + "|" + fakeKey).getBytes(StandardCharsets.UTF_8), value.array());
                added.add(fakeKey);
            }

            log.warn("TAMPERED compaction index: topic={} removed={} added={}", topic, removed, added);
            return HttpResponse.ok("{\"topic\":\"" + topic + "\",\"removedKeys\":" + toJson(removed)
                    + ",\"addedKeys\":" + toJson(added) + ",\"headUsedForFakes\":" + head + "}");
        } catch (Exception e) {
            log.error("tamper-index failed", e);
            return HttpResponse.serverError("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) return false;
        }
        return true;
    }

    private static String toJson(List<String> values) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append(values.get(i)).append('"');
        }
        return sb.append(']').toString();
    }
}
