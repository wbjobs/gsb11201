package com.gsb.cache;

/** Backing-store loader used by {@link CacheManager#get}. */
public interface Supplier {
    Object get();
}
