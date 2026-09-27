package com.gsb.cache;

/**
 * 可注入的时间源，单位毫秒。测试中可用 MutableClock 精确控制时间。
 */
public interface Clock {
    long currentTimeMillis();
}
