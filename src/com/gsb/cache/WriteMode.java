package com.gsb.cache;

/**
 * WRITE_THROUGH: every {@code put} is handed to the backing writer synchronously
 * before the call returns.
 * WRITE_BEHIND: every {@code put} is first appended to a bounded in-memory queue
 * and flushed by a background worker. Consecutive writes to the same key that are
 * still pending are coalesced into one flush.
 */
public enum WriteMode {
    WRITE_THROUGH,
    WRITE_BEHIND
}
