package com.gsb.cache;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * 单个缓存分区的内部实现：
 * L1 —— 容量受限的 LRU（LinkedHashMap access-order，外部同步）；
 * L2 —— 带精确 TTL 的并发哈希表（now &gt;= expiresAt 即过期）；
 * 单飞 —— 同一 key 的并发未命中只允许一个线程回源，其余等待其结果；
 * 版本号 —— invalidate/put 会递增 key 的版本，在途加载完成时若版本已变则丢弃结果，
 *          防止「失效之后旧值又被在途加载写回缓存」；
 * 写后 —— WRITE_BEHIND 下写入先进入有界合并队列，队列满时生产者阻塞（不丢数据），
 *          后台线程批量取出后刷写，同一 key 的多次写在队列中只保留最新值。
 */
final class Region {

    private static final class L2Entry {
        final Object value;
        final long expiresAt;

        L2Entry(Object value, long expiresAt) {
            this.value = value;
            this.expiresAt = expiresAt;
        }
    }

    private static final class InFlight {
        final long version;
        final CompletableFuture<Object> future = new CompletableFuture<Object>();

        InFlight(long version) {
            this.version = version;
        }
    }

    final String name;
    final int l1Capacity;
    final long l2TtlMillis;
    final WriteMode mode;
    final Clock clock;

    private final LinkedHashMap<String, Object> l1;
    private final ConcurrentHashMap<String, L2Entry> l2 = new ConcurrentHashMap<String, L2Entry>();
    private final ConcurrentHashMap<String, AtomicLong> versions = new ConcurrentHashMap<String, AtomicLong>();
    private final ConcurrentHashMap<String, InFlight> inflight = new ConcurrentHashMap<String, InFlight>();

    final Object queueLock = new Object();
    private final LinkedHashMap<String, Object> pending = new LinkedHashMap<String, Object>();
    volatile int queueCapacity;
    private volatile boolean flusherRunning;
    private Thread flusherThread;
    private volatile CacheWriter writer;

    private final AtomicLong l1Hits = new AtomicLong();
    private final AtomicLong l2Hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong loads = new AtomicLong();
    private final AtomicLong l1Evictions = new AtomicLong();
    private final AtomicLong invalidations = new AtomicLong();
    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong writeThroughWrites = new AtomicLong();
    private final AtomicLong writeBehindFlushes = new AtomicLong();
    private final AtomicLong writeBehindCoalesced = new AtomicLong();
    private final AtomicLong queueFullBlocks = new AtomicLong();

    Region(String name, int l1Capacity, long l2TtlMillis, WriteMode mode,
           Clock clock, int queueCapacity) {
        this.name = name;
        this.l1Capacity = l1Capacity;
        this.l2TtlMillis = l2TtlMillis;
        this.mode = mode;
        this.clock = clock;
        this.queueCapacity = queueCapacity;
        this.writer = new CacheWriter() {
            @Override
            public void write(String region, String key, Object value) {
                // 默认丢弃：未设置 CacheWriter 时写操作只落到缓存层。
            }
        };
        this.l1 = new LinkedHashMap<String, Object>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
                boolean evict = size() > Region.this.l1Capacity;
                if (evict) {
                    l1Evictions.incrementAndGet();
                }
                return evict;
            }
        };
        if (mode == WriteMode.WRITE_BEHIND) {
            flusherRunning = true;
            flusherThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    flusherLoop();
                }
            }, "gsb-cache-writebehind-" + name);
            flusherThread.setDaemon(true);
            flusherThread.start();
        }
    }

    void setWriter(CacheWriter writer) {
        if (writer == null) {
            throw new NullPointerException("writer");
        }
        this.writer = writer;
    }

    private long version(String key) {
        AtomicLong v = versions.get(key);
        return v == null ? 0L : v.get();
    }

    private void bumpVersion(String key) {
        AtomicLong v = versions.get(key);
        if (v == null) {
            AtomicLong fresh = new AtomicLong();
            v = versions.putIfAbsent(key, fresh);
            if (v == null) {
                v = fresh;
            }
        }
        v.incrementAndGet();
    }

    Object get(String key, Supplier<Object> loader) {
        for (;;) {
            if (l1Capacity > 0) {
                synchronized (l1) {
                    if (l1.containsKey(key)) {
                        l1Hits.incrementAndGet();
                        return l1.get(key);
                    }
                }
            }
            L2Entry entry = l2.get(key);
            if (entry != null) {
                if (clock.currentTimeMillis() < entry.expiresAt) {
                    l2Hits.incrementAndGet();
                    putL1(key, entry.value);
                    return entry.value;
                }
                l2.remove(key, entry);
            }
            misses.incrementAndGet();
            long versionAtStart = version(key);
            InFlight mine = new InFlight(versionAtStart);
            InFlight existing = inflight.putIfAbsent(key, mine);
            if (existing == null) {
                try {
                    loads.incrementAndGet();
                    Object value = loader.get();
                    if (value != null && version(key) == versionAtStart) {
                        putL1(key, value);
                        l2.put(key, new L2Entry(value, clock.currentTimeMillis() + l2TtlMillis));
                    }
                    mine.future.complete(value);
                    return value;
                } catch (Throwable t) {
                    mine.future.completeExceptionally(t);
                    if (t instanceof RuntimeException) {
                        throw (RuntimeException) t;
                    }
                    if (t instanceof Error) {
                        throw (Error) t;
                    }
                    throw new CacheException("loader failed for key " + key, t);
                } finally {
                    inflight.remove(key, mine);
                }
            }
            Object value = await(existing);
            if (version(key) == existing.version) {
                return value;
            }
            // 等待期间发生了 invalidate/put：在途结果已失效，重新走一遍流程。
        }
    }

    private static Object await(InFlight inFlight) {
        try {
            return inFlight.future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CacheException("interrupted while waiting for in-flight load", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new CacheException("in-flight load failed", cause);
        }
    }

    void put(String key, Object value) {
        if (value == null) {
            throw new NullPointerException("value");
        }
        bumpVersion(key);
        putL1(key, value);
        l2.put(key, new L2Entry(value, clock.currentTimeMillis() + l2TtlMillis));
        puts.incrementAndGet();
        if (mode == WriteMode.WRITE_THROUGH) {
            writeThroughWrites.incrementAndGet();
            writer.write(name, key, value);
        } else {
            enqueue(key, value);
        }
    }

    void invalidate(String key) {
        bumpVersion(key);
        invalidations.incrementAndGet();
        if (l1Capacity > 0) {
            synchronized (l1) {
                l1.remove(key);
            }
        }
        l2.remove(key);
    }

    private void putL1(String key, Object value) {
        if (l1Capacity <= 0 || value == null) {
            return;
        }
        synchronized (l1) {
            l1.put(key, value);
        }
    }

    private void enqueue(String key, Object value) {
        synchronized (queueLock) {
            while (pending.size() >= queueCapacity && !pending.containsKey(key)) {
                // 明确策略：队列满时生产者阻塞等待，绝不丢数据。
                queueFullBlocks.incrementAndGet();
                try {
                    queueLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new CacheException("interrupted while waiting for write-behind queue space", e);
                }
            }
            if (pending.containsKey(key)) {
                writeBehindCoalesced.incrementAndGet();
            }
            pending.put(key, value);
            queueLock.notifyAll();
        }
    }

    private void flusherLoop() {
        List<Map.Entry<String, Object>> carry = new ArrayList<Map.Entry<String, Object>>();
        for (;;) {
            List<Map.Entry<String, Object>> batch = new ArrayList<Map.Entry<String, Object>>(carry);
            carry.clear();
            synchronized (queueLock) {
                while (batch.isEmpty() && pending.isEmpty() && flusherRunning) {
                    try {
                        queueLock.wait();
                    } catch (InterruptedException ignored) {
                        // 忽略，靠 flusherRunning 标志退出。
                    }
                }
                if (pending.isEmpty() && !flusherRunning && batch.isEmpty()) {
                    return;
                }
                for (Map.Entry<String, Object> e : pending.entrySet()) {
                    batch.add(e);
                }
                pending.clear();
                queueLock.notifyAll();
            }
            for (Map.Entry<String, Object> e : batch) {
                try {
                    writer.write(name, e.getKey(), e.getValue());
                    writeBehindFlushes.incrementAndGet();
                } catch (Throwable t) {
                    // 刷写失败不丢数据：留到下一轮重试。
                    carry.add(e);
                    try {
                        Thread.sleep(50L);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    void shutdown() {
        flusherRunning = false;
        synchronized (queueLock) {
            queueLock.notifyAll();
        }
        if (flusherThread != null) {
            try {
                flusherThread.join(10000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    Stats snapshot() {
        long pendingSize;
        synchronized (queueLock) {
            pendingSize = pending.size();
        }
        return new Stats(l1Hits.get(), l2Hits.get(), misses.get(), loads.get(),
                l1Evictions.get(), invalidations.get(), puts.get(),
                writeThroughWrites.get(), writeBehindFlushes.get(),
                writeBehindCoalesced.get(), queueFullBlocks.get(), pendingSize);
    }

    List<String> l1Snapshot() {
        synchronized (l1) {
            return new ArrayList<String>(l1.keySet());
        }
    }
}
