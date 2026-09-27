package com.gsb.cache;

/**
 * 基于系统时间的 Clock 实现。
 */
public final class SystemClock implements Clock {
    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
