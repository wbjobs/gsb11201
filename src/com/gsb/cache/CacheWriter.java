package com.gsb.cache;

/**
 * Backing-store writer. Called synchronously in WRITE_THROUGH mode and by the
 * background flush worker in WRITE_BEHIND mode. Implementations must be
 * thread-safe.
 */
public interface CacheWriter {
    void write(String key, Object value);
}
