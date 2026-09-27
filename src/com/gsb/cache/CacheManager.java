package com.gsb.cache;

/**
 * Two-level cache:
 * <ul>
 *     <li>L1: per-region bounded LRU map (in-process).</li>
 *     <li>L2: per-region map with millisecond TTL boundaries.</li>
 * </ul>
 */
public interface CacheManager {
    void register(String region, int l1Capacity, long l2TtlMillis, WriteMode mode);

    /**
     * L1 hit, otherwise L2 hit (which promotes back into L1), otherwise the
     * loader is invoked. Concurrent misses for one key collapse to a single
     * loader call; all other callers wait for its result.
     */
    Object get(String region, String key, Supplier loader);

    void put(String region, String key, Object value);

    /** Removes the key from both L1 and L2 and blocks in-flight stale loads. */
    void invalidate(String region, String key);

    Stats stats(String region);
}
