package com.gsb.cache;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 多级缓存一致性管理器（L1 进程内 LRU + L2 带 TTL 的共享缓存）。
 *
 * 一致性语义：
 * 1. 同一 key 并发未命中时只有一次回源加载，其余请求等待该次结果（防击穿）；
 * 2. put 同时更新 L1/L2，WRITE_THROUGH 同步写底层存储，WRITE_BEHIND 进有界队列
 *    异步合并刷写（同 key 只保留最新值，队列满时阻塞生产者，不丢数据）；
 * 3. invalidate 同时清除 L1 和 L2，并通过递增 key 版本号使在途加载的结果失效，
 *    保证失效之后不会再读到旧值；
 * 4. 时间全部来自注入的 Clock，L2 在 now &gt;= 写入时间 + TTL 时精确过期。
 */
public class CacheManager implements AutoCloseable {

    /** WRITE_BEHIND 队列默认容量。 */
    public static final int DEFAULT_QUEUE_CAPACITY = 1024;

    private final Clock clock;
    private final ConcurrentHashMap<String, Region> regions = new ConcurrentHashMap<String, Region>();

    public CacheManager() {
        this(new SystemClock());
    }

    public CacheManager(Clock clock) {
        if (clock == null) {
            throw new NullPointerException("clock");
        }
        this.clock = clock;
    }

    /**
     * 注册一个缓存分区。
     *
     * @param region       分区名，不可重复
     * @param l1Capacity   L1（LRU）容量，&lt;=0 表示禁用 L1
     * @param l2TtlMillis  L2 条目的存活时间（毫秒）
     * @param mode         写模式
     */
    public synchronized void register(String region, int l1Capacity, long l2TtlMillis, WriteMode mode) {
        if (region == null) {
            throw new NullPointerException("region");
        }
        if (mode == null) {
            throw new NullPointerException("mode");
        }
        if (l2TtlMillis <= 0) {
            throw new IllegalArgumentException("l2TtlMillis must be positive");
        }
        if (regions.containsKey(region)) {
            throw new IllegalStateException("region already registered: " + region);
        }
        regions.put(region, new Region(region, l1Capacity, l2TtlMillis, mode, clock, DEFAULT_QUEUE_CAPACITY));
    }

    /**
     * 读取缓存，未命中时通过 loader 回源（同一 key 并发只回源一次）。
     */
    public Object get(String region, String key, Supplier<Object> loader) {
        if (loader == null) {
            throw new NullPointerException("loader");
        }
        return region(region).get(key, loader);
    }

    /**
     * 写入缓存并持久化（WRITE_THROUGH 同步，WRITE_BEHIND 异步合并）。
     */
    public void put(String region, String key, Object value) {
        region(region).put(key, value);
    }

    /**
     * 使某个 key 失效：同时清除 L1 和 L2，并使在途加载的结果失效。
     */
    public void invalidate(String region, String key) {
        region(region).invalidate(key);
    }

    /**
     * 返回该分区当前的统计快照。
     */
    public Stats stats(String region) {
        return region(region).snapshot();
    }

    /**
     * 设置底层存储写入器（WRITE_THROUGH 同步调用，WRITE_BEHIND 由刷写线程调用）。
     */
    public void setWriter(String region, CacheWriter writer) {
        region(region).setWriter(writer);
    }

    /**
     * 停止所有 WRITE_BEHIND 刷写线程，退出前会把队列中剩余数据刷完。
     */
    @Override
    public void close() {
        for (Region r : regions.values()) {
            r.shutdown();
        }
    }

    private Region region(String name) {
        Region r = regions.get(name);
        if (r == null) {
            throw new IllegalStateException("region not registered: " + name);
        }
        return r;
    }

    // ---- 以下方法仅供同包测试使用 ----

    Object writeBehindQueueLock(String region) {
        return region(region).queueLock;
    }

    List<String> l1Snapshot(String region) {
        return region(region).l1Snapshot();
    }

    void setWriteBehindQueueCapacity(String region, int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        region(region).queueCapacity = capacity;
    }
}
