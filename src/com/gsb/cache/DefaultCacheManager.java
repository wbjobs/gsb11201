package com.gsb.cache;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default {@link CacheManager} implementation.
 *
 * Consistency guarantees:
 * <ul>
 *     <li>Single-flight loading: concurrent misses for the same key share one
 *     loader invocation and its result.</li>
 *     <li>Invalidation/put epoch: each key carries an epoch that is bumped by
 *     {@code put}/{@code invalidate}. A load captures the epoch before reading
 *     the source and only publishes to L1/L2 when the epoch is unchanged; the
 *     epoch check and the cache writes are performed atomically together with
 *     the invalidation, so an in-flight load can never resurrect an old value
 *     after an invalidation.</li>
 *     <li>WRITE_BEHIND: pending writes are kept in a coalescing map (one slot
 *     per key holding the newest value) drained by a single worker. When the
 *     queue is full {@code put} applies back-pressure and blocks until space
 *     is available; data is never discarded.</li>
 *     <li>L1 eviction is strict LRU via an access-ordered {@link LinkedHashMap}.</li>
 *     <li>L2 entry is live while {@code now < expireAtMillis}; at the exact
 *     boundary millisecond it is expired.</li>
 * </ul>
 */
public class DefaultCacheManager implements CacheManager {

    private static final int DEFAULT_WRITE_BEHIND_QUEUE_CAPACITY = 1024;

    private final Clock clock;
    private final int writeBehindQueueCapacity;
    private final ConcurrentHashMap<String, Region> regions =
            new ConcurrentHashMap<String, Region>();

    public DefaultCacheManager(Clock clock) {
        this(clock, DEFAULT_WRITE_BEHIND_QUEUE_CAPACITY);
    }

    public DefaultCacheManager(Clock clock, int writeBehindQueueCapacity) {
        if (clock == null) {
            throw new NullPointerException("clock");
        }
        if (writeBehindQueueCapacity < 1) {
            throw new IllegalArgumentException(
                    "writeBehindQueueCapacity must be >= 1");
        }
        this.clock = clock;
        this.writeBehindQueueCapacity = writeBehindQueueCapacity;
    }

    @Override
    public void register(String region, int l1Capacity, long l2TtlMillis,
            WriteMode mode) {
        if (region == null) {
            throw new NullPointerException("region");
        }
        if (l1Capacity < 0) {
            throw new IllegalArgumentException("l1Capacity must be >= 0");
        }
        if (l2TtlMillis <= 0L) {
            throw new IllegalArgumentException("l2TtlMillis must be > 0");
        }
        if (mode == null) {
            throw new NullPointerException("mode");
        }
        Region r = new Region(region, l1Capacity, l2TtlMillis, mode);
        if (regions.putIfAbsent(region, r) != null) {
            throw new IllegalArgumentException("region already registered: " + region);
        }
        if (mode == WriteMode.WRITE_BEHIND) {
            r.startWorker();
        }
    }

    private Region region(String name) {
        Region r = regions.get(name);
        if (r == null) {
            throw new IllegalArgumentException("region not registered: " + name);
        }
        return r;
    }

    @Override
    public Object get(String region, String key, Supplier loader) {
        if (key == null) {
            throw new NullPointerException("key");
        }
        if (loader == null) {
            throw new NullPointerException("loader");
        }
        Region r = region(region);

        Object cached = r.l1Get(key);
        if (cached != null) {
            r.l1Hits.incrementAndGet();
            return cached;
        }

        L2Entry entry = r.l2.get(key);
        if (entry != null) {
            if (clock.currentTimeMillis() < entry.expireAtMillis) {
                r.l2Hits.incrementAndGet();
                r.l1Put(key, entry.value);
                return entry.value;
            }
            r.l2.remove(key, entry);
        }

        r.misses.incrementAndGet();

        LoadFuture own = new LoadFuture();
        LoadFuture inFlight = r.inFlight.putIfAbsent(key, own);
        if (inFlight != null) {
            return inFlight.await();
        }
        try {
            long epochBeforeLoad = r.currentEpoch(key);
            Object value;
            try {
                value = loader.get();
            } catch (RuntimeException ex) {
                own.fail(ex);
                throw ex;
            }
            r.loads.incrementAndGet();
            if (value != null) {
                synchronized (r.invalidationLock) {
                    if (r.currentEpoch(key) == epochBeforeLoad) {
                        r.l1Put(key, value);
                        r.l2Put(key, value);
                    }
                }
            }
            own.complete(value);
            return value;
        } finally {
            r.inFlight.remove(key, own);
        }
    }

    @Override
    public void put(String region, String key, Object value) {
        if (key == null) {
            throw new NullPointerException("key");
        }
        if (value == null) {
            throw new NullPointerException("value");
        }
        Region r = region(region);
        r.puts.incrementAndGet();
        synchronized (r.invalidationLock) {
            r.bumpEpoch(key);
            r.l1Put(key, value);
            r.l2Put(key, value);
        }
        if (r.mode == WriteMode.WRITE_THROUGH) {
            r.writer.write(key, value);
            r.flushes.incrementAndGet();
        } else {
            r.enqueue(key, value);
        }
    }

    @Override
    public void invalidate(String region, String key) {
        if (key == null) {
            throw new NullPointerException("key");
        }
        Region r = region(region);
        r.invalidations.incrementAndGet();
        synchronized (r.invalidationLock) {
            r.bumpEpoch(key);
            r.l1Remove(key);
            r.l2.remove(key);
        }
    }

    @Override
    public Stats stats(String region) {
        Region r = region(region);
        return new Stats(r.l1Hits.get(), r.l2Hits.get(), r.misses.get(),
                r.loads.get(), r.puts.get(), r.invalidations.get(),
                r.l1Evictions.get(), r.flushes.get(), r.l1Size(), r.l2.size(),
                r.pendingSize());
    }

    /** Installs the backing writer (no-op by default). */
    public void setWriter(String region, CacheWriter writer) {
        if (writer == null) {
            throw new NullPointerException("writer");
        }
        region(region).writer = writer;
    }

    /**
     * Hook invoked by the WRITE_BEHIND worker before every drain. Intended for
     * tests that need to freeze flushing deterministically.
     */
    public void setPreFlushHook(String region, Runnable hook) {
        region(region).preFlushHook = hook;
    }

    /** Synchronously flushes every pending WRITE_BEHIND entry for the region. */
    public void flush(String region) {
        region(region).drainPending();
    }

    /** Stops worker threads and synchronously flushes any remaining data. */
    public void shutdown() {
        for (Region r : regions.values()) {
            r.shutdown();
        }
    }

    /** One load in flight for a key; waiters park on its latch. */
    private static final class LoadFuture {
        private final CountDownLatch done = new CountDownLatch(1);
        private Object value;
        private RuntimeException error;

        void complete(Object v) {
            value = v;
            done.countDown();
        }

        void fail(RuntimeException e) {
            error = e;
            done.countDown();
        }

        Object await() {
            boolean interrupted = false;
            while (true) {
                try {
                    done.await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            if (error != null) {
                throw error;
            }
            return value;
        }
    }

    private static final class L2Entry {
        final Object value;
        final long expireAtMillis;

        L2Entry(Object value, long expireAtMillis) {
            this.value = value;
            this.expireAtMillis = expireAtMillis;
        }
    }

    private final class Region {
        final String name;
        final int l1Capacity;
        final long l2TtlMillis;
        final WriteMode mode;

        final AtomicLong l1Hits = new AtomicLong();
        final AtomicLong l2Hits = new AtomicLong();
        final AtomicLong misses = new AtomicLong();
        final AtomicLong loads = new AtomicLong();
        final AtomicLong puts = new AtomicLong();
        final AtomicLong invalidations = new AtomicLong();
        final AtomicLong l1Evictions = new AtomicLong();
        final AtomicLong flushes = new AtomicLong();

        final LinkedHashMap<String, Object> l1;
        final ConcurrentHashMap<String, L2Entry> l2 =
                new ConcurrentHashMap<String, L2Entry>();
        final ConcurrentHashMap<String, LoadFuture> inFlight =
                new ConcurrentHashMap<String, LoadFuture>();
        final ConcurrentHashMap<String, AtomicLong> epochs =
                new ConcurrentHashMap<String, AtomicLong>();
        final Object invalidationLock = new Object();

        final Object queueLock = new Object();
        final LinkedHashMap<String, Object> pending = new LinkedHashMap<String, Object>();
        volatile CacheWriter writer = new CacheWriter() {
            @Override
            public void write(String key, Object value) {
                // no-op default writer
            }
        };
        volatile Runnable preFlushHook;
        volatile boolean stopped;
        private Thread worker;

        Region(String name, int l1Capacity, long l2TtlMillis, WriteMode mode) {
            this.name = name;
            this.l1Capacity = l1Capacity;
            this.l2TtlMillis = l2TtlMillis;
            this.mode = mode;
            this.l1 = new LinkedHashMap<String, Object>(16, 0.75f, true) {
                private static final long serialVersionUID = 1L;
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
                    if (size() > Region.this.l1Capacity) {
                        Region.this.l1Evictions.incrementAndGet();
                        return true;
                    }
                    return false;
                }
            };
        }

        Object l1Get(String key) {
            if (l1Capacity == 0) {
                return null;
            }
            synchronized (l1) {
                return l1.get(key);
            }
        }

        void l1Put(String key, Object value) {
            if (l1Capacity == 0) {
                return;
            }
            synchronized (l1) {
                l1.put(key, value);
            }
        }

        void l1Remove(String key) {
            synchronized (l1) {
                l1.remove(key);
            }
        }

        int l1Size() {
            synchronized (l1) {
                return l1.size();
            }
        }

        void l2Put(String key, Object value) {
            l2.put(key, new L2Entry(value, clock.currentTimeMillis() + l2TtlMillis));
        }

        long currentEpoch(String key) {
            AtomicLong epoch = epochs.get(key);
            return epoch == null ? 0L : epoch.get();
        }

        void bumpEpoch(String key) {
            AtomicLong epoch = epochs.computeIfAbsent(key,
                    new java.util.function.Function<String, AtomicLong>() {
                        @Override
                        public AtomicLong apply(String k) {
                            return new AtomicLong();
                        }
                    });
            epoch.incrementAndGet();
        }

        // ---- WRITE_BEHIND queue ----

        void enqueue(String key, Object value) {
            synchronized (queueLock) {
                while (!stopped
                        && pending.size() >= writeBehindQueueCapacity
                        && !pending.containsKey(key)) {
                    try {
                        queueLock.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(
                                "interrupted while waiting for write-behind queue space", e);
                    }
                }
                if (!stopped) {
                    pending.put(key, value);
                    queueLock.notifyAll();
                    return;
                }
            }
            // After shutdown, fall back to a direct synchronous write so no
            // data is ever lost.
            writer.write(key, value);
            flushes.incrementAndGet();
        }

        void startWorker() {
            worker = new Thread(new Runnable() {
                @Override
                public void run() {
                    workerLoop();
                }
            }, "gsb-cache-writebehind-" + name);
            worker.setDaemon(true);
            worker.start();
        }

        private void workerLoop() {
            while (true) {
                synchronized (queueLock) {
                    while (pending.isEmpty() && !stopped) {
                        try {
                            queueLock.wait();
                        } catch (InterruptedException e) {
                            if (stopped) {
                                return;
                            }
                        }
                    }
                    if (pending.isEmpty() && stopped) {
                        return;
                    }
                }

                Runnable hook = preFlushHook;
                if (hook != null) {
                    hook.run();
                }

                Map<String, Object> batch;
                synchronized (queueLock) {
                    if (pending.isEmpty()) {
                        if (stopped) {
                            return;
                        }
                        continue;
                    }
                    batch = new LinkedHashMap<String, Object>(pending);
                    pending.clear();
                    queueLock.notifyAll();
                }
                writeBatch(batch);
            }
        }

        void writeBatch(Map<String, Object> batch) {
            Iterator<Map.Entry<String, Object>> it = batch.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Object> e = it.next();
                writer.write(e.getKey(), e.getValue());
                flushes.incrementAndGet();
            }
        }

        void drainPending() {
            Map<String, Object> batch;
            synchronized (queueLock) {
                if (pending.isEmpty()) {
                    return;
                }
                batch = new LinkedHashMap<String, Object>(pending);
                pending.clear();
                queueLock.notifyAll();
            }
            writeBatch(batch);
        }

        int pendingSize() {
            synchronized (queueLock) {
                return pending.size();
            }
        }

        void shutdown() {
            if (mode != WriteMode.WRITE_BEHIND) {
                stopped = true;
                return;
            }
            synchronized (queueLock) {
                stopped = true;
                queueLock.notifyAll();
            }
            if (worker != null) {
                boolean interrupted = false;
                long deadline = 5000L;
                while (deadline > 0L) {
                    long start = System.currentTimeMillis();
                    try {
                        worker.join(deadline);
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                    deadline -= System.currentTimeMillis() - start;
                    if (!worker.isAlive()) {
                        break;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            drainPending();
        }
    }
}
