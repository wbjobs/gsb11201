package com.gsb.cache;

/**
 * 底层存储写入接口。WRITE_THROUGH 下同步调用，WRITE_BEHIND 下由后台刷写线程调用。
 */
public interface CacheWriter {
    void write(String region, String key, Object value);
}
