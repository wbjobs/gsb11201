package com.gsb.cache;

/**
 * 测试用的可手动推进的 Clock。
 */
public final class MutableClock implements Clock {
    private long now;

    public MutableClock(long initial) {
        this.now = initial;
    }

    @Override
    public synchronized long currentTimeMillis() {
        return now;
    }

    public synchronized void set(long millis) {
        this.now = millis;
    }

    public synchronized void advance(long deltaMillis) {
        this.now += deltaMillis;
    }
}
