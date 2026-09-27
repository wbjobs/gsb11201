package com.gsb.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * 纯 main + 断言的验收测试，不依赖 JUnit。由 run-tests.sh 以 -ea 启动。
 */
public final class TestRunner {

    public static void main(String[] args) throws Exception {
        String[] names = {
                "concurrentMissSingleLoad",
                "writeBehindCoalescesTenWrites",
                "l1EvictionOrderIsLru",
                "invalidateRacingWithInFlightLoad",
                "l2TtlBoundary",
                "writeThroughIsSynchronous",
                "writeBehindFullQueueBlocksWithoutDataLoss"
        };
        int failures = 0;
        for (int i = 0; i < names.length; i++) {
            try {
                switch (i) {
                    case 0:
                        testConcurrentMissSingleLoad();
                        break;
                    case 1:
                        testWriteBehindCoalescesTenWrites();
                        break;
                    case 2:
                        testL1EvictionOrderIsLru();
                        break;
                    case 3:
                        testInvalidateRacingWithInFlightLoad();
                        break;
                    case 4:
                        testL2TtlBoundary();
                        break;
                    case 5:
                        testWriteThroughIsSynchronous();
                        break;
                    case 6:
                        testWriteBehindFullQueueBlocksWithoutDataLoss();
                        break;
                    default:
                        throw new AssertionError("unknown test");
                }
                System.out.println("PASS " + names[i]);
            } catch (Throwable t) {
                failures++;
                System.out.println("FAIL " + names[i]);
                t.printStackTrace(System.out);
            }
        }
        if (failures != 0) {
            System.out.println(failures + " test(s) failed");
            System.exit(1);
        }
        System.out.println("ALL TESTS PASSED");
    }

    // 用例 1：同一 key 并发未命中，32 个线程同时 get，回源只允许发生一次。
    private static void testConcurrentMissSingleLoad() throws Exception {
        final CacheManager cm = new CacheManager();
        cm.register("r", 100, 60000L, WriteMode.WRITE_THROUGH);
        final AtomicInteger loadCount = new AtomicInteger();
        Supplier<Object> loader = new Supplier<Object>() {
            @Override
            public Object get() {
                loadCount.incrementAndGet();
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "V";
            }
        };
        int threads = 32;
        final CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<Object>> futures = new ArrayList<Future<Object>>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(new SupplierTask(cm, barrier, loader)));
        }
        for (Future<Object> f : futures) {
            check("V".equals(f.get(10, TimeUnit.SECONDS)), "all threads must get V");
        }
        pool.shutdownNow();
        check(loadCount.get() == 1, "loader must run exactly once, was " + loadCount.get());
        check(cm.stats("r").getLoads() == 1, "stats loads must be 1, was " + cm.stats("r").getLoads());
        cm.close();
    }

    private static final class SupplierTask implements java.util.concurrent.Callable<Object> {
        private final CacheManager cm;
        private final CyclicBarrier barrier;
        private final Supplier<Object> loader;

        SupplierTask(CacheManager cm, CyclicBarrier barrier, Supplier<Object> loader) {
            this.cm = cm;
            this.barrier = barrier;
            this.loader = loader;
        }

        @Override
        public Object call() throws Exception {
            barrier.await();
            return cm.get("r", "hot-key", loader);
        }
    }

    // 用例 2：WRITE_BEHIND 连续十次写同一 key，队列里合并成一条，只刷一次。
    private static void testWriteBehindCoalescesTenWrites() throws Exception {
        final CacheManager cm = new CacheManager();
        cm.register("r", 100, 60000L, WriteMode.WRITE_BEHIND);
        final CountingWriter writer = new CountingWriter();
        cm.setWriter("r", writer);
        // 持住队列锁完成十次写入，刷写线程在此期间无法取出：十条写入在队列中合并成一条。
        synchronized (cm.writeBehindQueueLock("r")) {
            for (int i = 0; i < 10; i++) {
                cm.put("r", "k", "v" + i);
            }
        }
        check(writer.firstFlush.await(5, TimeUnit.SECONDS), "first flush timed out");
        waitFor(new Condition() {
            @Override
            public boolean holds() {
                Stats s = cm.stats("r");
                return s.getPendingWrites() == 0 && s.getWriteBehindFlushes() >= 1;
            }
        }, 5000L);
        Thread.sleep(150L);
        Stats s = cm.stats("r");
        check(s.getWriteBehindFlushes() == 1, "expect exactly 1 flush, was " + s.getWriteBehindFlushes());
        check("v9".equals(writer.lastValue.get()), "flush must carry latest value, was " + writer.lastValue.get());
        check("v9".equals(cm.get("r", "k", new Supplier<Object>() {
            @Override
            public Object get() {
                throw new AssertionError("k should be served from cache");
            }
        })), "cached value must be v9");
        cm.close();
    }

    // 用例 3：L1 容量 3，访问/写入触发淘汰，淘汰顺序必须严格符合 LRU。
    private static void testL1EvictionOrderIsLru() {
        final CacheManager cm = new CacheManager();
        cm.register("r", 3, 60000L, WriteMode.WRITE_THROUGH);
        Supplier<Object> never = new Supplier<Object>() {
            @Override
            public Object get() {
                throw new AssertionError("L2 should still hold the evicted value");
            }
        };
        cm.put("r", "a", "A");
        cm.put("r", "b", "B");
        cm.put("r", "c", "C");
        check("A".equals(cm.get("r", "a", never)), "get a"); // 顺序变为 b,c,a
        cm.put("r", "d", "D"); // 淘汰 b
        check("C".equals(cm.get("r", "c", never)), "get c"); // 顺序变为 a,d,c
        cm.put("r", "e", "E"); // 淘汰 a
        check(cm.stats("r").getL1Evictions() == 2,
                "expect 2 L1 evictions, was " + cm.stats("r").getL1Evictions());
        check(cm.l1Snapshot("r").equals(Arrays.asList("d", "c", "e")),
                "LRU order must be [d,c,e], was " + cm.l1Snapshot("r"));
        check("D".equals(cm.get("r", "d", never)), "d still in L1");
        check("C".equals(cm.get("r", "c", never)), "c still in L1");
        check("E".equals(cm.get("r", "e", never)), "e still in L1");
        check("B".equals(cm.get("r", "b", never)), "b evicted from L1 but still in L2");
        check("A".equals(cm.get("r", "a", never)), "a evicted from L1 but still in L2");
        Stats s = cm.stats("r");
        check(s.getLoads() == 0, "no loader invocation expected, loads=" + s.getLoads());
        check(s.getL2Hits() == 2, "expect 2 L2 hits, was " + s.getL2Hits());
        cm.close();
    }

    // 用例 4：invalidate 与在途加载竞争。第一次加载在途中执行 invalidate 并放行，
    // 在途结果（旧值）绝不允许写回缓存；失效之后的读取只能拿到重新加载的新值。
    private static void testInvalidateRacingWithInFlightLoad() throws Exception {
        final CacheManager cm = new CacheManager();
        cm.register("r", 100, 60000L, WriteMode.WRITE_THROUGH);
        final AtomicInteger loadCount = new AtomicInteger();
        final CountDownLatch loadEntered = new CountDownLatch(1);
        final CountDownLatch releaseLoad = new CountDownLatch(1);
        Supplier<Object> loader = new Supplier<Object>() {
            @Override
            public Object get() {
                int n = loadCount.incrementAndGet();
                if (n == 1) {
                    loadEntered.countDown();
                    try {
                        releaseLoad.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "OLD";
                }
                return "NEW";
            }
        };
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<Object> first = pool.submit(new java.util.concurrent.Callable<Object>() {
            @Override
            public Object call() {
                return cm.get("r", "k", loader);
            }
        });
        check(loadEntered.await(5, TimeUnit.SECONDS), "first load never started");
        cm.invalidate("r", "k");
        // T2 在 invalidate 之后、第一次加载返回之前发起读：可能等到旧结果，但必须重试拿新值。
        Future<Object> second = pool.submit(new java.util.concurrent.Callable<Object>() {
            @Override
            public Object call() {
                return cm.get("r", "k", loader);
            }
        });
        Thread.sleep(50L); // 确保 T2 已经挂在同一个在途加载上
        releaseLoad.countDown();
        check("OLD".equals(first.get(5, TimeUnit.SECONDS)), "leader returns what it loaded");
        check("NEW".equals(second.get(5, TimeUnit.SECONDS)),
                "reader after invalidate must not see OLD value");
        check(loadCount.get() == 2, "expect exactly 2 loads, was " + loadCount.get());
        // 再多读几次，缓存中永远是 NEW，且不再回源。
        for (int i = 0; i < 5; i++) {
            Object v = cm.get("r", "k", new Supplier<Object>() {
                @Override
                public Object get() {
                    throw new AssertionError("NEW must be cached");
                }
            });
            check("NEW".equals(v), "subsequent reads must return NEW, got " + v);
        }
        check(loadCount.get() == 2, "still expect exactly 2 loads, was " + loadCount.get());
        check(cm.stats("r").getInvalidations() == 1, "invalidations must be 1");
        pool.shutdownNow();
        cm.close();
    }

    // 用例 5：L2 TTL 边界精确——到期前一毫秒仍命中，到期时刻恰好过期重新加载。
    private static void testL2TtlBoundary() {
        MutableClock clock = new MutableClock(1000L);
        final CacheManager cm = new CacheManager(clock);
        cm.register("r", 0, 500L, WriteMode.WRITE_THROUGH); // L1 禁用，只验 L2
        final AtomicInteger loads = new AtomicInteger();
        Supplier<Object> loader = new Supplier<Object>() {
            @Override
            public Object get() {
                return "v" + loads.incrementAndGet();
            }
        };
        check("v1".equals(cm.get("r", "k", loader)), "first miss loads v1 at t=1000");
        clock.set(1499L);
        check("v1".equals(cm.get("r", "k", loader)), "t=1499 still within TTL, must hit L2");
        check(loads.get() == 1, "loads must remain 1 before boundary, was " + loads.get());
        clock.set(1500L);
        check("v2".equals(cm.get("r", "k", loader)), "t=1500 == writeTime+ttl must be expired");
        check(loads.get() == 2, "loads must be 2 at boundary, was " + loads.get());
        // put 路径同样精确：t=1500 写入，TTL 500，2000 时刻恰好过期。
        cm.put("r", "p", "x");
        clock.set(1999L);
        check("x".equals(cm.get("r", "p", loader)), "put entry valid at t=1999");
        clock.set(2000L);
        check("v3".equals(cm.get("r", "p", loader)), "put entry expired at t=2000, reload");
        check(loads.get() == 3, "loads must be 3, was " + loads.get());
        cm.close();
    }

    // 用例 6：WRITE_THROUGH 每次 put 都同步、立即落到底层写入器。
    private static void testWriteThroughIsSynchronous() {
        final CacheManager cm = new CacheManager();
        cm.register("r", 100, 60000L, WriteMode.WRITE_THROUGH);
        final CountingWriter writer = new CountingWriter();
        cm.setWriter("r", writer);
        cm.put("r", "a", 1);
        cm.put("r", "b", 2);
        check(writer.flushes.get() == 2, "write-through must call writer synchronously twice, was "
                + writer.flushes.get());
        check(cm.stats("r").getWriteThroughWrites() == 2, "stats writeThroughWrites must be 2");
        check(Integer.valueOf(1).equals(cm.get("r", "a", new Supplier<Object>() {
            @Override
            public Object get() {
                throw new AssertionError("a should be cached");
            }
        })), "a readable from cache");
        cm.close();
    }

    // 用例 7：WRITE_BEHIND 队列满时生产者阻塞等待（明确策略，不丢数据），
    // 刷写线程腾出空间后写入继续，所有写入最终全部刷到底层。
    private static void testWriteBehindFullQueueBlocksWithoutDataLoss() throws Exception {
        final CacheManager cm = new CacheManager();
        cm.register("r", 100, 60000L, WriteMode.WRITE_BEHIND);
        cm.setWriteBehindQueueCapacity("r", 2);
        final GatedWriter writer = new GatedWriter();
        cm.setWriter("r", writer);
        cm.put("r", "k1", "1");
        check(writer.entered.await(5, TimeUnit.SECONDS), "flusher never picked k1");
        // 刷写线程卡在写 k1；队列里 k2、k3 填满容量 2。
        cm.put("r", "k2", "2");
        cm.put("r", "k3", "3");
        ExecutorService one = Executors.newSingleThreadExecutor();
        Future<Object> blockedPut = one.submit(new java.util.concurrent.Callable<Object>() {
            @Override
            public Object call() {
                cm.put("r", "k4", "4");
                return "done";
            }
        });
        Thread.sleep(300L);
        check(!blockedPut.isDone(), "put must block when queue is full instead of dropping data");
        check(cm.stats("r").getQueueFullBlocks() >= 1, "queue-full block must be counted");
        writer.release.countDown();
        check("done".equals(blockedPut.get(5, TimeUnit.SECONDS)), "blocked put must finish after drain");
        waitFor(new Condition() {
            @Override
            public boolean holds() {
                return cm.stats("r").getPendingWrites() == 0
                        && writer.written.size() == 4;
            }
        }, 5000L);
        Collections.sort(writer.written);
        check(writer.written.equals(Arrays.asList("k1", "k2", "k3", "k4")),
                "all 4 writes must be flushed, was " + writer.written);
        check(cm.stats("r").getWriteBehindFlushes() == 4,
                "4 distinct keys mean 4 flushes, was " + cm.stats("r").getWriteBehindFlushes());
        one.shutdownNow();
        cm.close();
    }

    private interface Condition {
        boolean holds() throws Exception;
    }

    private static void waitFor(Condition condition, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.holds()) {
                return;
            }
            Thread.sleep(10L);
        }
        if (!condition.holds()) {
            throw new AssertionError("condition not met within " + timeoutMillis + "ms");
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static final class CountingWriter implements CacheWriter {
        final AtomicInteger flushes = new AtomicInteger();
        final AtomicReference<Object> lastValue = new AtomicReference<Object>();
        final CountDownLatch firstFlush = new CountDownLatch(1);

        @Override
        public void write(String region, String key, Object value) {
            flushes.incrementAndGet();
            lastValue.set(value);
            firstFlush.countDown();
        }
    }

    private static final class GatedWriter implements CacheWriter {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final List<String> written = Collections.synchronizedList(new ArrayList<String>());

        @Override
        public void write(String region, String key, Object value) {
            written.add(key);
            entered.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
