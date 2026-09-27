package com.gsb.cache;

/**
 * Injectable time source. All time-dependent behavior of the cache
 * (L2 expiry, async processing timestamps) is read through this interface.
 */
public interface Clock {
    long currentTimeMillis();
}
