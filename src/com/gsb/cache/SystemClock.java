package com.gsb.cache;

/** Clock backed by {@link System#currentTimeMillis()}. */
public final class SystemClock implements Clock {
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
