package com.gsb.cache.tests;

import com.gsb.cache.Clock;
import java.util.concurrent.atomic.AtomicLong;

/** Controllable clock for deterministic TTL tests. */
final class MutableClock implements Clock {
    private final AtomicLong nowMillis;

    MutableClock(long startMillis) {
        this.nowMillis = new AtomicLong(startMillis);
    }

    public long currentTimeMillis() {
        return nowMillis.get();
    }

    void setTime(long millis) {
        nowMillis.set(millis);
    }

    void advance(long deltaMillis) {
        nowMillis.addAndGet(deltaMillis);
    }
}
